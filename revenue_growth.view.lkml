view: revenue_growth {
  derived_table: {
    sql:
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

    -- aggregate to week and month
          week_aggregated as (
              select
                  date_trunc(dt, week) as week,
                  account_id,
                  sum(invoice) as week_invoice
              from base
              group by 1,2
          ),

          month_aggregated as (
              select
                  date_trunc(dt, month) as month,
                  account_id,
                  parent_logo__c,
                  ge__c,
                  name,
                  type_of_customer__c,
                  churn_date__c,
                   resurrected_date__c,
                   parent_customertype,
                  sum(invoice) as invoice
              from base
              group by 1,2,3,4,5,6,7,8,9
              having invoice > 0 -- for edge cases id '0011U00000Ouun4QAB' who was charged and refunded on 2019-8-15, so its monthly fee cancelled out
          ),

     -- Take first payment date, defines our cohorts
          first_dt as (
              select
                  account_id,
                  min(dt) as first_dt,
                  date_trunc(min(dt), week) as first_week,
                  date_trunc(min(dt), month) as first_month
              from base
              group by 1
          ),

    -- Append first payment month (start date) to each account
          month_aggregated_append as (
              select
                  m.*,
                  f.first_month
              from month_aggregated m join first_dt f
              using (account_id)
          ),

     -- calculate revenue growth
     -- has to be at month-account-ge__c granularity, we lost all the individual billing, invoice info here
          revenue_growth as (
           select
            coalesce(tm.account_id, lm.account_id) as account,
          -- payment date for this month
            coalesce(tm.month, date_add(lm.month, interval 1 month)) as month,
            coalesce(tm.parent_logo__c, lm.parent_logo__c) as parent_accountid,
            coalesce(tm.ge__c, lm.ge__c) as company_stripe_id,
            coalesce(tm.name, lm.name) as name,
            coalesce(tm.type_of_customer__c, lm.type_of_customer__c) as customer_type,
            coalesce(tm.churn_date__c, lm.churn_date__c) as churn_date,
            coalesce(tm.resurrected_date__c, lm.resurrected_date__c) as resurrected_date,
            coalesce(tm.parent_customertype, lm.parent_customertype) as parent_customer_type,

          -- sum up this month's invoice as revenue
            tm.invoice as revenue,

          -- retained revenue from last month
                -- if this month received more, use last month as retained (retained and gained some)
                 case when tm.account_id is not NULL and lm.account_id is not NULL and tm.invoice >= lm.invoice
                 then lm.invoice
                 -- if this month received less, use this month as retained (retained but lost some)
                 when tm.account_id is not NULL and lm.account_id is not NULL and tm.invoice < lm.invoice
                 then tm.invoice
                 else 0 end
                 as retained,

          -- new revenue from newly onboarded clients
               -- if first payment month is this month, then new revenue
                case when tm.first_month = tm.month then tm.invoice
                else 0 end
                as new_,

          -- existing clients spent more for this month
                case when tm.month != tm.first_month
                    and tm.account_id is not NULL and lm.account_id is not NULL
                    and tm.invoice > lm.invoice
                    and lm.invoice > 0 then tm.invoice - lm.invoice
                else 0 end
                as expansion,

          -- churned clients came back for this month
                case when tm.account_id is not NULL
                -- last month did not pay
                    and (lm.account_id is NULL or lm.invoice = 0)
                -- this month paid and it's not the first time paying
                    and tm.invoice > 0 and tm.first_month != tm.month
                    then tm.invoice
                else 0 end
                as resurrected,

          -- existing clients spent less for this month
            -1 *
                (case when tm.month != tm.first_month
                     and tm.account_id is not NULL and lm.account_id is not NULL
                     and tm.invoice < lm.invoice and tm.invoice > 0
                     then lm.invoice - tm.invoice
                else 0 end) as contraction,

          -- churned clients
            -1 * (
                case when lm.invoice > 0 and (tm.account_id is NULL or tm.invoice = 0)
                then lm.invoice else 0 end
            ) as churned

        from
        -- this month
            month_aggregated_append tm
            full outer join
        -- last month
            month_aggregated_append lm
            on (tm.account_id = lm.account_id
                and date_add(lm.month, interval 1 month) = tm.month
            )
       order by 1,2
          )

          select * from revenue_growth
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: account {
    type: string
    sql: ${TABLE}.account ;;
  }

  dimension: month {
    type: date
    datatype: date
    sql: ${TABLE}.month ;;
  }

  dimension: parent_accountid {
    type: string
    sql: ${TABLE}.parent_accountid ;;
  }

  dimension: company_stripe_id {
    type: string
    sql: ${TABLE}.company_stripe_id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: customer_type {
    type: string
    sql: ${TABLE}.customer_type ;;
  }

  dimension: churn_date {
    type: string
    sql: ${TABLE}.churn_date ;;
  }

  dimension: resurrected_date {
    type: string
    sql: ${TABLE}.resurrected_date ;;
  }

  dimension: parent_customer_type {
    type: string
    sql: ${TABLE}.parent_customer_type ;;
  }

  dimension: parent_agency_identifier {
    type: yesno
    sql: ${TABLE}.parent_customer_type = 'Agency' ;;
  }

  measure: account_revenue {
    type: number
    sql: ${TABLE}.revenue ;;
  }

  measure: revenue {
    type:  sum
    sql: ${TABLE}.revenue ;;

  }

  measure: account_retained {
    type: number
    sql: ${TABLE}.retained ;;
    drill_fields: [detail*]
  }

  measure: retained {
    type:  sum
    sql: ${TABLE}.retained ;;

  }

  measure: account_new {
    type: number
    sql: ${TABLE}.new_ ;;
  }

  measure: new {
    type:  sum
    sql: ${TABLE}.new_ ;;

  }

  measure: account_expansion {
    type: number
    sql: ${TABLE}.expansion ;;
  }

  measure: expansion {
    type:  sum
    sql: ${TABLE}.new_ ;;

  }

  measure: account_resurrected {
    type: number
    sql: ${TABLE}.resurrected ;;
  }

  measure: resurrected {
    type:  sum
    sql: ${TABLE}.resurrected ;;

  }

  measure: account_contraction {
    type: number
    sql: ${TABLE}.contraction ;;
  }

  measure: contraction {
    type:  sum
    sql: ${TABLE}.contraction ;;

  }

  measure: account_churned {
    type: number
    sql: ${TABLE}.churned ;;
  }

  measure: churned {
    type:  sum
    sql: ${TABLE}.churned ;;

  }

  set: detail {
    fields: [
      account,
      month,
      parent_accountid,
      company_stripe_id,
      name,
      customer_type,
      churn_date,
      resurrected_date,
      parent_customer_type,
      account_revenue,
      account_retained,
      account_new,
      account_expansion,
      account_resurrected,
      account_contraction,
      account_churned
    ]
  }
}
