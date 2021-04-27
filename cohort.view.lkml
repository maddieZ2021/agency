view: cohort {
  derived_table: {
    sql: -- Transaction data breakdown
      -- account_id: id of the customer, 1158 customers in total
      -- payment_id: id of the payment, one customer can make multiple payments
      -- payment_usd: amount of each payment, note there are $0 payments, one customer can pay $0 for a certain month
      -- payment_date: date of the payment, customer doesn't pay multiple times on the same day, one user - one paymentdate - one payment

       With abs as (
                select distinct * from
                    (select
                      account__c,
                      bill.id as billing_id,  -- unique for each invoice,
                      invoiceid__c as invoice_id, -- unique for each invoice, but may be null
                      name as payment_name, -- same as billing id, unique
                      amount__c AS invoice,
                      refund_reason__c,
                      DATE(date2__c) AS stripe_created_invoice_date,  -- is it?
                      description__c,
                      notes__c,
                      isdsp__c,
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
                    isdsp__c,
                    parent_logo__c,
                    dedup.ge__c,
                    dedup.name,
                    dedup.type_of_customer__c,
                    dedup.churn_date__c,
                    dedup.resurrected_date__c,
                    dd.type_of_customer__c as parent_customertype
                 from abs
                left join dedup
                on abs.account__c= dedup.id
                left join dedup as dd
                on abs.parent_logo__c = dd.id
                --where dd.type_of_customer__c = 'Agency'
                ),

      -- 1. aggregate payment by month for each customer, granularity -> user_id
         agg_month as (
             Select
               account_id as user_id,
               date_trunc(dt, month) as payment_month,
               sum(invoice) as monthly_usd
             From base
             Group by user_id, payment_month),

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
               a.user_id,
               a.payment_month,
               a.monthly_usd,
               f.first_payment_month
             From agg_month a
             join first_month f
             on a.user_id = f.user_id
             Where a.monthly_usd != 0),

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
              a1.first_payment_month,
              a1.payment_month,
              a2.cohort_size_fixed,
              count(distinct a1.user_id) as cohort_size_changing,
              sum(a1.monthly_usd) as cohort_usd
          From agg_month_withfirst a1
          join agg_month_cohortsize a2
          on a1.first_payment_month = a2.first_payment_month
            Group by a1.first_payment_month, a1.payment_month, a2.cohort_size_fixed),

      -- 6. get months since first payment month, append to each cohort group, granularity -> payment_month + first_payment_month
        agg_month_sincefirst as (
          Select
            first_payment_month,
            payment_month,
            cohort_size_fixed,
            cohort_size_changing,
            cohort_usd,
            date_diff(payment_month, first_payment_month, month) as months_since_first
          From agg_month_withsize)


      Select * from agg_month_sincefirst
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: first_payment_month {
    type: date
    datatype: date
    sql: ${TABLE}.first_payment_month ;;
  }

  dimension: payment_month {
    type: date
    datatype: date
    sql: ${TABLE}.payment_month ;;
  }

  dimension: months_since_first {
    type: number
    sql: ${TABLE}.months_since_first ;;
  }

  dimension: cohort_size_fixed {
    type: number
    sql: ${TABLE}.cohort_size_fixed ;;
  }

  dimension: cohort_size_changing {
    type: number
    sql: ${TABLE}.cohort_size_changing ;;
  }

  dimension: cohort_monthly_pay {
    type: number
    sql: ${TABLE}.cohort_monthly_pay ;;
  }

  measure: cumm_sum {
    type:sum
    sql: ${TABLE}.cohort_usd ;;
  }

  set: detail {
    fields: [
      first_payment_month,
      payment_month,
      months_since_first,
      cohort_size_fixed,
      cohort_size_changing,
      cohort_monthly_pay,
      cumm_sum
    ]
  }
}
