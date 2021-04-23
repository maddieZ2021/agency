view: mrr {
  derived_table: {
    sql:
    -- stripe to SF invoice
    With abs as (
          select distinct * from
              (select
                bill.lastmodifieddate,
                account__c,
                bill.id as billing_id,  -- unique for each invoice
                amount__c AS invoice,
                DATE(date2__c) AS stripe_created_invoice_date,  -- is it?
                description__c,
                parent_logo__c,
                rank() over (partition by  account__c, bill.id order by bill.lastmodifieddate desc) as rank
                FROM `pogon-155405.salesforce_to_bigquery.Payments__c` bill
               where isdeleted is False
               and amount__c != 0
               and amount__c is not null
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
                     (SELECT *, row_number() over (partition by id  order by lastmodifieddate desc) as row_number
                     FROM `pogon-155405.salesforce_to_bigquery.Account` ) b
                where b.row_number = 1 )),

    -- append account info to invoice
        base as
          (select
              account__c as account_id,
              billing_id,
              stripe_created_invoice_date as dt,
              invoice as invoice,
              description__c,
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
          where dd.type_of_customer__c = 'Agency'),

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
                  sum(invoice) as invoice
              from base
              group by 1,2
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
                  m.month,
                  m.account_id,
                  m.invoice,
                  f.first_month
              from month_aggregated m join first_dt f
              using (account_id)
          ),

     -- calculate revenue growth
          revenue_growth as (
           select
          -- payment date for this month
            coalesce(tm.month, date_add(lm.month, interval 1 month)) as month,

          -- sum up this month's invoice as revenue
            sum(tm.invoice) as revenue,

          -- retained revenue from last month
            sum(
                -- if this month received more, use last month as retained (retained and gained some)
                 case when tm.account_id is not NULL and lm.account_id is not NULL and tm.invoice >= lm.invoice
                 then lm.invoice
                 -- if this month received less, use this month as retained (retained but lost some)
                 when tm.account_id is not NULL and lm.account_id is not NULL and tm.invoice < lm.invoice
                 then tm.invoice
                 else 0 end
               ) as retained,

          -- new revenue from newly onboarded clients
            sum(
               -- if first payment month is this month, then new revenue
                case when tm.first_month = tm.month then tm.invoice
                else 0 end
            ) as new_,

          -- existing clients spent more for this month
            sum(
                case when tm.month != tm.first_month
                    and tm.account_id is not NULL and lm.account_id is not NULL
                    and tm.invoice > lm.invoice
                    and lm.invoice > 0 then tm.invoice - lm.invoice
                else 0 end
            ) as expansion,

          -- churned clients came back for this month
            sum(
                case when tm.account_id is not NULL
                -- last month did not pay
                    and (lm.account_id is NULL or lm.invoice = 0)
                -- this month paid and it's not the first time paying
                    and tm.invoice > 0 and tm.first_month != tm.month
                    then tm.invoice
                else 0 end
            ) as resurrected,

          -- existing clients spent less for this month
            -1 * sum(
                case when tm.month != tm.first_month
                     and tm.account_id is not NULL and lm.account_id is not NULL
                     and tm.invoice < lm.invoice and tm.invoice > 0
                     then lm.invoice - tm.invoice
                else 0 end
            ) as contraction,

          -- churned clients
            -1 * sum(
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
       group by 1
       order by 1
          )

          select * from revenue_growth
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: month {
    type: date_month
    datatype: date
    sql: ${TABLE}.month ;;
  }

  measure: revenue {
    type: sum
    sql: ${TABLE}.revenue ;;
  }

  measure: retained {
    type: sum
    sql: ${TABLE}.retained ;;
  }

  measure: new {
    type: sum
    sql: ${TABLE}.new_ ;;
  }

  measure: expansion {
    type: sum
    sql: ${TABLE}.expansion ;;
  }

  measure: resurrected {
    type: sum
    sql: ${TABLE}.resurrected ;;
  }

  measure: contraction {
    type: sum
    sql: ${TABLE}.contraction ;;
  }

  measure: churned {
    type: sum
    sql: ${TABLE}.churned ;;
  }

  # filter: parent_agency_identifier {
  #   type: string
  #   sql: {% condition parent_agency %}  {% endcondition %} ;;
  # }

  set: detail {
    fields: [
      month,
      revenue,
      retained,
      new,
      expansion,
      resurrected,
      contraction,
      churned
    ]
  }
}
