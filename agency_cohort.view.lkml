view: agency_cohort {
  derived_table: {
    sql: With abs as
      (select distinct * from
          (select
            account__c,
            bill.id as billing_id,  -- unique for each invoice,
            invoiceid__c as invoice_id, -- unique for each invoice, but may be null
            name as payment_name, -- same as billing id, unique
            amount__c AS invoice,
            refund_reason__c,
            DATE(date2__c) AS stripe_created_invoice_date,
            description__c,
            notes__c,
            parent_logo__c,
            rank() over (partition by  account__c, bill.id order by bill.lastmodifieddate desc) as rank
            FROM `pogon-155405.salesforce_to_bigquery.Payments__c` bill
           where isdeleted is False
           and amount__c != 0
           and amount__c is not null
           and account__c != '0011U00001ekFIFQA2' -- for resurrected edge case: ge__c = '6048'
           and (((parent_logo__c is null and description__c like '%Media Fee%') and
                (parent_logo__c is null and description__c not like 'Target-Media Cost'))
                or parent_logo__c is not null)
           )
          where rank = 1),

-- deduplicated SF account info
      dedup as
       (select distinct
           id,
           ge__c, -- company stripe ID
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
            and id != '0011U00001ekFIFQA2' -- for resurrected edge case: ge__c = '6048'
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
        --where dd.type_of_customer__c = 'Agency'
        ),

  -- some invoices under same account are missing parent account info (i.e. account_id = '0011U00001L7AAUQA3' for march)
  -- so we are filling in missing data
      account_info as (
         select distinct
                account_id,
                parent_logo__c,
                ge__c,
                name,
                type_of_customer__c,
                churn_date__c,
                resurrected_date__c,
                parent_customertype,
                parent_name
          from base where parent_logo__c is not null),

-- 1. aggregate payment by month for each customer, granularity -> user_id
   agg_month as (
       Select
         account_id as user_id,
         date_trunc(dt, month) as payment_month,
         COALESCE(b.parent_logo__c, a.parent_logo__c) as parent_logo__c,
         COALESCE(b.parent_customertype, a.parent_customertype) as parent_customertype,
         COALESCE(b.parent_name, a.parent_name) as parent_name,
         sum(invoice) as monthly_usd
       From base b
       left join account_info a
       using (account_id)
       Group by 1,2,3,4,5),

-- 4. calculate initial cohort size, group customers by their first_payment_month
      agg_month_cohort as (
        Select
          parent_name,
          min(payment_month) as first_payment_month,
          count(distinct user_id) as cohort_size_fixed
        From agg_month
        Group by 1
        ),

-- 5. aggregate to payment_month - first_payment_month granularity, trace the changing cohort_size by payment month
      agg_month_withsize as (
        Select
            user_id,
            payment_month,
            parent_logo__c,
            parent_customertype,
            first_payment_month,
            a1.parent_name,
            a2.cohort_size_fixed,
            count(distinct a1.user_id) as cohort_size_changing, -- will return 1 for each record, we will sum them up later in defining lookml measures
            sum(a1.monthly_usd) as cohort_usd
        From agg_month a1
        join agg_month_cohort a2
        using (parent_name)
        Group by 1,2,3,4,5,6,7),

-- 6. get months since first payment month, append to each cohort group, granularity -> payment_month + first_payment_month
        agg_month_sincefirst as (
          Select
            payment_month,
            parent_logo__c,
            parent_customertype,
            first_payment_month,
            parent_name,
            cohort_size_fixed,
            cohort_size_changing,
            round(cohort_usd, 0) as revenue,
            sum(cohort_usd) over (partition by parent_name
                                  order by payment_month RANGE UNBOUNDED PRECEDING) as cumm_sum,
            date_diff(payment_month, first_payment_month, month) as months_since_first
          From agg_month_withsize a)

      Select * from agg_month_sincefirst where parent_customertype like 'Agency%'
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: payment_month {
    type: date
    datatype: date
    sql: ${TABLE}.payment_month ;;
  }

  dimension: parent_logo__c {
    type: string
    sql: ${TABLE}.parent_logo__c ;;
  }

  dimension: parent_customertype {
    type: string
    sql: ${TABLE}.parent_customertype ;;
  }

  dimension: first_payment_month {
    type: date
    datatype: date
    sql: ${TABLE}.first_payment_month ;;
  }

  dimension: parent_name {
    type: string
    sql: ${TABLE}.parent_name ;;
  }

  dimension: cohort_size_fixed {
    type: number
    sql: ${TABLE}.cohort_size_fixed ;;
  }

  dimension: cohort_size_changing {
    type: number
    sql: ${TABLE}.cohort_size_changing ;;
  }

  dimension: revenue {
    type: number
    sql: ${TABLE}.revenue ;;
  }

  dimension: cumm_sum {
    type: number
    sql: ${TABLE}.cumm_sum ;;
  }

  dimension: months_since_first {
    type: number
    sql: ${TABLE}.months_since_first ;;
  }


  ## new
  measure: sum_revenue {
    type: average # cuz the cumm_sum from agg_month_sincefirst repeated for all accounts who shared the same first_pay_date and actual pay_date
    sql: ${TABLE}.cumm_sum ;;
    drill_fields: [detail*]
    value_format: "$0"
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
      payment_month,
      parent_logo__c,
      parent_customertype,
      first_payment_month,
      parent_name,
      cohort_size_fixed,
      revenue,
      cumm_sum,
      months_since_first
    ]
  }
}
