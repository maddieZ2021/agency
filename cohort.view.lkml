view: cohort {
  derived_table: {
    sql:
  With abs as (
  -- deduplicate Stripe - SF invoice info
        select distinct * from
            (select
              account__c,
              bill.id as billing_id,  -- unique for each invoice, same as 18 digit payment ID in SF
              invoiceid__c as invoice_id, -- unique for each invoice, but may be null
              name as payment_name, -- same as billing id, unique
              amount__c AS invoice,
              refund_reason__c,
              DATE(date2__c) AS stripe_created_invoice_date,
              description__c,
              notes__c,
              parent_logo__c,
              isdeleted,
              row_number() over (partition by  account__c, bill.id order by bill.lastmodifieddate desc) as rank -- use row_number, donot use rank()
              FROM `pogon-155405.salesforce_to_bigquery.Payments__c` bill
             -- where isdeleted is not TRUE
            ) b
             where rank = 1
             and parent_logo__c is not null -- to eliminate bill.id a0l1U000006leVDQAY
             and invoice != 0
             and invoice is not null
             and account__c not in ( '0011U00001ekFIFQA2', -- for resurrected edge case: ge__c = '6048'
                                     '0011U00001AnixWQAR', -- for perpetua test $1 per month
                                     '0011U00001gKy44QAC') -- for edge case: accountid no longer exist
             and isdeleted is not true  -- for edge case: 2021-01-31 18975 and 18997 redundant
             and (((parent_logo__c is null and description__c like '%Media Fee%') and  -- count amazon media fee
                  (parent_logo__c is null and description__c not like 'Target-Media Cost'))  -- count target media fee
                  or parent_logo__c is not null) -- all managed adspend + platform fee
             ),

    -- deduplicate SF account info
        dedup as
           (select distinct
             id,
             ge__c, -- company stripe ID, may be null
             name,
             type_of_customer__c,
             churn_date__c,
             resurrected_date__c
            from
               (select *
                from
                     (SELECT *, row_number() over (partition by id order by lastmodifieddate desc) as row_number
                     FROM `pogon-155405.salesforce_to_bigquery.Account` ) b
                where b.row_number = 1
                and id not in ('0011U00001ekFIFQA2', -- for resurrected edge case: ge__c = '6048'
                               '0011U00001AnixWQAR', -- for perpetua test $1 per month
                               '0011U00001gKy44QAC') -- for edge case: accountid no longer exist
                and isdeleted is not true
                )),

  -- append account info to invoice
      base as
        (select
            account__c as account_id,
            stripe_created_invoice_date as dt,
            billing_id,
            invoice_id,
            invoice,
            payment_name,
            description__c,
            refund_reason__c,
            notes__c,
            parent_logo__c,
            dedup.ge__c,
            dedup.name,
            dedup.type_of_customer__c,
            dedup.churn_date__c,
            dedup.resurrected_date__c,
            dd.type_of_customer__c as parent_customertype,
            dd.name as parent_name
         from abs
        left join dedup
        on abs.account__c= dedup.id
        left join dedup as dd
        on abs.parent_logo__c = dd.id
        ),


-- 1. aggregate payment by month for each customer, granularity -> user_id
   agg_month_temp as (
       Select
         account_id as user_id,
         date_trunc(dt, month) as payment_month,
         parent_logo__c,
         ge__c,
         name,
         type_of_customer__c,
         parent_customertype,
         parent_name,
         sum(invoice) as monthly_usd
       From base
       where {% condition parent_customertype %} parent_customertype {% endcondition %}
       Group by 1,2,3,4,5,6,7,8
       having sum(invoice) > 0), -- refund balanced out some invoices

-- take most recent month's revenue to classify accounts by revenue tiers
    tier as (
       select
           user_id,
           case when monthly_usd <= 500 then 'Growth: <= 500'
           when monthly_usd > 500 and monthly_usd <= 1500 then 'Pro: 500~1500'
           when monthly_usd > 1500 and monthly_usd <= 4000 then 'Pro+: 1500~4000'
           when monthly_usd > 4000 then 'Enterprise: > 4000'
           else null end as revenue_tier
       from (select *
             from
                (select
                   user_id, monthly_usd, payment_month,
                   row_number() over (partition by user_id order by payment_month desc) as most_recent
                 from agg_month_temp
                 where monthly_usd is not null
                 and monthly_usd != 0
                 and payment_month <= date_trunc(current_date(), month)) a -- do not consider future payments (the one time payment for future months)
             where a.most_recent = 1)),

     agg_month as (
        select *
        from agg_month_temp
        left join
        tier
        using (user_id)),

-- 2. get first month of payment for each customer, granularity -> user_id
     first_month as (
         Select
           user_id,
           date_trunc(min(payment_month), month) as first_payment_month
         From agg_month
         Group by user_id),

-- 3. append first month of payment to agg_month, filter out $0 payments, granularity -> user_id
     agg_month_withfirst as (
         Select
           a.*,
           f.first_payment_month
         From agg_month a
         join first_month f
         on a.user_id = f.user_id
         Where a.monthly_usd != 0
         and {% condition revenue_tier %} revenue_tier {% endcondition %}),

-- 4. calculate initial cohort size, group customers by their first_payment_month
      agg_month_cohortsize as (
        Select
          first_payment_month,
          count(distinct user_id) as cohort_size_fixed
        From agg_month_withfirst
        Group by first_payment_month
        ),

-- 5. aggregate to payment_month - first_payment_month granularity, trace the changing cohort_size by payment month
      agg_month_withsize as (
        Select
            user_id,
            payment_month,
            parent_logo__c,
            ge__c,
            name,
            type_of_customer__c,
            parent_customertype,
            revenue_tier,
            parent_name,
            a2.cohort_size_fixed,
            a1.first_payment_month,
            count(distinct a1.user_id) as cohort_size_changing, -- will return 1 for each record, we will sum them up later while defining lookml measures
            sum(a1.monthly_usd) as cohort_usd
        From agg_month_withfirst a1
        join agg_month_cohortsize a2
        on a1.first_payment_month = a2.first_payment_month
        Group by 1,2,3,4,5,6,7,8,9,10,11),

-- 6. get months since first payment month, append to each cohort group, granularity -> payment_month + first_payment_month
        agg_month_sincefirst as (
          Select
            a.* except(cohort_usd),
            round(cohort_usd, 0) as revenue,
            sum(cohort_usd) over (partition by first_payment_month
                                  order by payment_month RANGE UNBOUNDED PRECEDING) as cumm_sum,
            date_diff(payment_month, first_payment_month, month) as months_since_first
          From agg_month_withsize a
          where payment_month < date_trunc(current_date(), month)) -- for heatmap we only look at past months, or else incomplete billing cycle (current month) will skew the color coding

      Select * from agg_month_sincefirst;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: SF_account_id {
    label: "account"
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: payment_month {
    type: date
    datatype: date
    sql: ${TABLE}.payment_month ;;
  }

  dimension: parent_SF_accountid {
    label: "parent_accountid"
    type: string
    sql: ${TABLE}.parent_logo__c ;;
  }

  dimension: company_stripe_id {
    type: string
    sql: ${TABLE}.ge__c ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: type_of_customer {
    label: "customer_type"
    type: string
    sql: ${TABLE}.type_of_customer__c ;;
  }


  dimension: cohort_size_fixed {
    type: number
    sql: ${TABLE}.cohort_size_fixed ;;
  }

  dimension: first_payment_month {
    type: date
    datatype: date
    sql: ${TABLE}.first_payment_month ;;
  }

  dimension: months_since_first {
    type: number
    sql: ${TABLE}.months_since_first ;;
  }

  dimension: cohort_size_changing {
    type: number
    sql: ${TABLE}.cohort_size_changing ;;
  }

  dimension: account_revenue {
    label: "revenue"
    type: number
    sql: ${TABLE}.revenue ;;
  }

  measure: sum_revenue {
    type: average # cuz the cumm_sum from agg_month_sincefirst repeated for all accounts who shared the same first_pay_date and actual pay_date
    sql: ${TABLE}.cumm_sum ;;
    drill_fields: [detail*]
    value_format: "$0"
  }

  dimension: parent_name {
    type: string
    sql: ${TABLE}.parent_name ;;
  }

#add
# for filter
  dimension: parent_customertype {
    type: string
    sql: ${TABLE}.parent_customertype ;;
  }

  dimension: revenue_tier {
    type: string
    sql: ${TABLE}.revenue_tier ;;
  }

# for retained percent
  measure: cohort_size {
    type: sum
    sql: ${cohort_size_changing} ;; # sum up all the 1s
    drill_fields: [detail*]
    link: {
      label: "Explore Top revenue by account for this cohort"
      url: "{{ link }}&sorts=cohort.account_revenue+desc"
    }
  }
  measure: remained_percent {
    type: number
    sql: ${cohort_size}/${cohort_size_fixed};;
    drill_fields: [detail*]
    value_format: "0.0%"
  }

# for LTV
  measure: cohort_fixed {
    type: average
    sql: ${TABLE}.cohort_size_fixed ;;
  }

  measure: LTV {
    type: number
    sql: ${sum_revenue}/${cohort_fixed} ;;
    drill_fields: [detail*]
    value_format: "$0"
  }

  set: detail {
    fields: [
      parent_customertype,
      parent_SF_accountid,
      parent_name,
      SF_account_id,
      revenue_tier,
      company_stripe_id,
      name,
      type_of_customer,
      cohort_size_fixed,
      account_revenue,
      months_since_first
    ]
  }
}
