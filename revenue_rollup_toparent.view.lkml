view: revenue_rollup_toparent {
  derived_table: {
    sql: With abs as (
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

            -- aggregate to month-account level
                month_aggregated as (
                    select
                        date_trunc(dt, month) as month,
                        parent_logo__c,
                        parent_customertype,
                        parent_name,
                        sum(invoice) as invoice
                    from base b
                    group by 1,2,3,4
                    having invoice > 0 -- for edge cases id '0011U00000Ouun4QAB' who was charged and refunded on 2019-8-15, so its monthly fee cancelled out
                ),
            -- by now every month - account_id pair should have only one aggregated invoice except for 'Your Super' who was billing 2 parents from 2020-02 to 2020-05'

           -- Take first payment date, defines our cohorts
                first_dt as (
                    select
                        parent_logo__c,
                        date_trunc(min(dt), month) as first_month
                    from base
                    group by 1
                ),

          -- Append first payment month to each account
                month_aggregated_append as (
                    select
                        m.*,
                        f.first_month
                    from month_aggregated m
                    join first_dt f
                    using (parent_logo__c)
                ),

           -- calculate revenue growth
           -- has to be at month-account granularity, we lost all the individual billing, invoice info here
                final as (
                  select
                      coalesce(tm.month, date_add(lm.month, interval 1 month)) as month,
                      coalesce(tm.parent_logo__c, lm.parent_logo__c) as parent_accountid,
                      coalesce(tm.parent_customertype, lm.parent_customertype) as parent_customer_type,
                      coalesce(tm.parent_name, lm.parent_name) as parent_name,
                    -- sum up this month's invoice as revenue
                      sum(tm.invoice) as revenue,
                    -- retained revenue from last month
                    -- if this month received more, use last month as retained (retained and gained some)
                       sum(case when tm.parent_logo__c is not NULL and lm.parent_logo__c is not NULL and tm.invoice >= lm.invoice
                       then lm.invoice
                    -- if this month received less, use this month as retained (retained but lost some)
                       when tm.parent_logo__c is not NULL and lm.parent_logo__c is not NULL and tm.invoice < lm.invoice
                       then tm.invoice
                       else 0 end)
                       as retained,

                    -- new revenue from newly onboarded clients
                    -- if first payment month is this month, then new revenue
                      sum(case when tm.first_month = tm.month then tm.invoice
                      else 0 end)
                      as new_,

                    -- existing clients spent more for this month
                      sum(case when tm.month != tm.first_month
                          and tm.parent_logo__c is not NULL and lm.parent_logo__c is not NULL
                          and tm.invoice > lm.invoice
                          and lm.invoice > 0 then tm.invoice - lm.invoice
                      else 0 end)
                      as expansion,

                    -- churned clients came back this month
                      sum(case when tm.parent_logo__c is not NULL
                    -- last month did not pay
                          and (lm.parent_logo__c is NULL or lm.invoice = 0)
                    -- this month paid and it's not the first time paying
                          and tm.invoice > 0 and tm.first_month != tm.month
                          then tm.invoice
                      else 0 end)
                      as resurrected,

                    -- existing clients spent less for this month
                   -1 * sum(case when tm.month != tm.first_month
                           and tm.parent_logo__c is not NULL and lm.parent_logo__c is not NULL
                           and tm.invoice < lm.invoice and tm.invoice > 0
                           then lm.invoice - tm.invoice
                      else 0 end) as contraction,

                -- churned clients
                   -1 * sum( case when lm.invoice > 0 and (tm.parent_logo__c is NULL or tm.invoice = 0)
                             then lm.invoice
                             else 0 end) as churned

                  from
                  -- this month
                      month_aggregated_append tm
                      full outer join
                  -- last month
                      month_aggregated_append lm
                      on (tm.parent_logo__c = lm.parent_logo__c
                          and date_add(lm.month, interval 1 month) = tm.month
                          and tm.parent_logo__c = lm.parent_logo__c
                          -- for edge case accountid 0011U00000NNLQEQA5 who had 2 invoiced billed to 2 parent ids in the same month
                      )
                  group by 1,2,3,4
                  order by 1,2)

            select * from final where month <= date_trunc(current_date(), month)
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
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

  dimension: parent_customer_type {
    type: string
    sql: ${TABLE}.parent_customer_type ;;
  }

  dimension: parent_name {
    type: string
    sql: ${TABLE}.parent_name ;;
  }

  dimension: account_revenue {
    type: number
    sql: ${TABLE}.revenue ;;
  }

  dimension: account_retained {
    type: number
    sql: ${TABLE}.retained ;;
  }

  dimension: account_new {
    type: number
    sql: ${TABLE}.new_ ;;
  }

  dimension: account_expansion {
    type: number
    sql: ${TABLE}.expansion ;;
  }

  dimension: account_resurrected {
    type: number
    sql: ${TABLE}.resurrected ;;
  }

  dimension: account_contraction {
    type: number
    sql: ${TABLE}.contraction ;;
  }

  dimension: account_churned {
    type: number
    sql: ${TABLE}.churned ;;
  }

# aggregated measures
  measure: revenue {
    type:  sum
    sql: ${account_revenue};;
    value_format: "$#.00;($#.00)"

    drill_fields: [detail*]
    link: {
      label: "Explore Top Revenue by account"
      url: "{{ link }}&sorts=SF_account_revenue.account_revenue+desc"
    }
  }
  measure: retained {
    type:  sum
    sql: ${account_retained} ;;
    value_format: "$#.00;($#.00)"

    drill_fields: [detail*]
    link: {
      label: "Explore Top Retained Revenue by account"
      url: "{{ link }}&sorts=SF_account_revenue.account_retained+desc"
    }
  }
  measure: new {
    type:  sum
    sql: ${account_new} ;;
    value_format: "$#.00;($#.00)"

    drill_fields: [detail*]
    link: {
      label: "Explore Top New Revenue by account"
      url: "{{ link }}&sorts=SF_account_revenue.account_new+desc"
    }
  }
  measure: expansion {
    type:  sum
    sql: ${account_expansion} ;;
    value_format: "$#.00;($#.00)"

    drill_fields: [detail*]
    link: {
      label: "Explore Top Expansion by account"
      url: "{{ link }}&sorts=SF_account_revenue.account_expansion+desc"
    }
  }
  measure: resurrected {
    type:  sum
    sql: ${account_resurrected} ;;
    value_format: "$#.00;($#.00)"

    drill_fields: [detail*]
    link: {
      label: "Explore Top Resurrected Revenue by account"
      url: "{{ link }}&sorts=SF_account_revenue.account_resurrected+desc"
    }
  }
  measure: contraction {
    type:  sum
    sql: ${account_contraction} ;;
    value_format: "$#.00;($#.00)"

    drill_fields: [detail*]
    link: {
      label: "Explore Worst Contraction by account"
      url: "{{ link }}&sorts=SF_account_revenue.account_contraction+asc"
    }
  }
  measure: churned {
    type:  sum
    sql: ${account_churned} ;;
    value_format: "$#.00;($#.00)"

    drill_fields: [detail*]
    link: {
      label: "Explore Worst Churn by account"
      url: "{{ link }}&sorts=SF_account_revenue.account_churned+asc"
    }
  }

  set: detail {
    fields: [
      month,
      parent_accountid,
      parent_customer_type,
      parent_name,
      revenue,
      retained,

      expansion,
      resurrected,
      contraction,
      churned
    ]
  }
}
