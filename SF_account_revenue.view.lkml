view: SF_account_revenue {
  derived_table: {
    sql:
    With abs as (
    -- deduplicate Stripe - SF invoice info
          select distinct * from
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

    -- deduplicate SF account info
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
            from base where parent_logo__c is not null),

      -- aggregate to month-account level
        month_aggregated as (
              select
                  date_trunc(b.dt, month) as month,
                  b.account_id,
                  COALESCE(b.parent_logo__c, a.parent_logo__c) as parent_logo__c,
                  COALESCE(b.ge__c, a.ge__c) as ge__c,
                  COALESCE(b.name, a.name) as name,
                  COALESCE(b.type_of_customer__c, a.type_of_customer__c) as type_of_customer__c,
                  COALESCE(b.churn_date__c, a.churn_date__c) as churn_date__c,
                  COALESCE(b.resurrected_date__c, a.resurrected_date__c) as resurrected_date__c,
                  COALESCE(b.parent_customertype, a.parent_customertype) as parent_customertype,
                  sum(invoice) as invoice
              from base b
              left join account_info a
              using (account_id)
              group by 1,2,3,4,5,6,7,8,9
              having invoice > 0 -- for edge cases id '0011U00000Ouun4QAB' who was charged and refunded on 2019-8-15, so its monthly fee cancelled out
          ),


     -- Take first payment date, defines our cohorts
          first_dt as (
              select
                  account_id,
                  min(dt) as first_dt,
                  date_trunc(min(dt), month) as first_month
              from base
              group by 1
          ),

    -- Append first payment month (start date) to each account
          month_aggregated_append as (
              select
                  m.*,
                  f.first_month
              from month_aggregated m
              join first_dt f
              using (account_id)
          ),

     -- calculate revenue growth
     -- has to be at month-account granularity, we lost all the individual billing, invoice info here
          revenue_growth as (
            select
                coalesce(tm.account_id, lm.account_id) as account,
                coalesce(tm.month, date_add(lm.month, interval 1 month)) as month,
                coalesce(tm.parent_logo__c, lm.parent_logo__c) as parent_accountid,
                coalesce(tm.ge__c, lm.ge__c) as company_stripe_id,
                coalesce(tm.name, lm.name) as name,
                coalesce(tm.type_of_customer__c, lm.type_of_customer__c) as customer_type,
                coalesce(tm.churn_date__c, lm.churn_date__c) as churn_date,
                coalesce(tm.resurrected_date__c, lm.resurrected_date__c) as resurrected_date,
                coalesce(tm.parent_customertype, lm.parent_customertype) as parent_customer_type,

              -- sum up this month's invoice as revenue
                sum(tm.invoice) as revenue,
              -- retained revenue from last month
              -- if this month received more, use last month as retained (retained and gained some)
                 sum(case when tm.account_id is not NULL and lm.account_id is not NULL and tm.invoice >= lm.invoice
                 then lm.invoice
              -- if this month received less, use this month as retained (retained but lost some)
                 when tm.account_id is not NULL and lm.account_id is not NULL and tm.invoice < lm.invoice
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
                    and tm.account_id is not NULL and lm.account_id is not NULL
                    and tm.invoice > lm.invoice
                    and lm.invoice > 0 then tm.invoice - lm.invoice
                else 0 end)
                as expansion,

              -- churned clients came back this month
                sum(case when tm.account_id is not NULL
              -- last month did not pay
                    and (lm.account_id is NULL or lm.invoice = 0)
              -- this month paid and it's not the first time paying
                    and tm.invoice > 0 and tm.first_month != tm.month
                    then tm.invoice
                else 0 end)
                as resurrected,

              -- existing clients spent less for this month
             -1 * sum(case when tm.month != tm.first_month
                     and tm.account_id is not NULL and lm.account_id is not NULL
                     and tm.invoice < lm.invoice and tm.invoice > 0
                     then lm.invoice - tm.invoice
                else 0 end) as contraction,

          -- churned clients
             -1 * sum( case when lm.invoice > 0 and (tm.account_id is NULL or tm.invoice = 0)
                       then lm.invoice
                       else 0 end) as churned

            from
            -- this month
                month_aggregated_append tm
                full outer join
            -- last month
                month_aggregated_append lm
                on (tm.account_id = lm.account_id
                    and date_add(lm.month, interval 1 month) = tm.month
                )
            group by 1,2,3,4,5,6,7,8,9
            order by 1,2),

     -- take most recent month's revenue (not current month) to classify accounts by revenue tiers
          revenue_tier as (
             select
                 account,
                 case when revenue <= 500 then 'Growth: <= 500'
                 when revenue > 500 and revenue <= 1500 then 'Pro: 500~1500'
                 when revenue > 1500 and revenue <= 4000 then 'Pro+: 1500~4000 '
                 when revenue > 4000 then 'Enterprise: > 4000'
                 else null end as revenue_tier
             from (select *
                   from
                      (select
                         account, revenue, month,
                         row_number() over (partition by account order by month desc) as most_recent
                       from revenue_growth
                       where revenue is not null
                       and revenue != 0) a
                   where a.most_recent = 1) b),

          final as (
              select * from
              revenue_growth
              left join
              revenue_tier
              using (account))

      select * from final ;;
  }

  dimension: account {
    type: string
    sql: ${TABLE}.account ;;
  }

  dimension: payment_date {
    type: string
    sql: ${month} ;;
  }

  dimension: month {
    type: date_month
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

# for drill-in to work
  dimension: has_revenue {
    type: yesno
    sql: ${account_revenue} != 0 ;;
  }
  dimension: has_retained {
    type: yesno
    sql: ${account_retained} != 0 ;;
  }
  dimension: has_new {
    type: yesno
    sql: ${account_new} != 0 ;;
  }
  dimension: has_expansion {
    type: yesno
    sql: ${account_expansion} != 0 ;;
  }
  dimension: has_resurrected {
    type: yesno
    sql: ${account_resurrected} != 0 ;;
  }
  dimension: has_contraction {
    type: yesno
    sql: ${account_contraction} != 0 ;;
  }
  dimension: has_churned {
    type: yesno
    sql: ${account_churned} != 0 ;;
  }
  dimension: has_remained {
    type: yesno
    sql: ${account_retained} != 0 and ${account_expansion} = 0 and ${account_contraction} = 0  ;;
  }



# account level dimensions from derived table
  dimension: account_revenue {
    type: number
    sql: ${TABLE}.revenue ;;
    value_format: "$#.00;($#.00)"
  }
  dimension: account_retained {
    type: number
    sql: ${TABLE}.retained ;;
    value_format: "$#.00;($#.00)"
  }
  dimension: account_new {
    type: number
    sql: ${TABLE}.new_ ;;
    value_format: "$#.00;($#.00)"
  }
  dimension: account_expansion {
    type: number
    sql: ${TABLE}.expansion ;;
    value_format: "$#.00;($#.00)"
  }
  dimension: account_resurrected {
    type: number
    sql: ${TABLE}.resurrected ;;
    value_format: "$#.00;($#.00)"
  }
  dimension: account_contraction {
    type: number
    sql: ${TABLE}.contraction ;;
    value_format: "$#.00;($#.00)"
  }
  dimension: account_churned {
    type: number
    sql: ${TABLE}.churned ;;
    value_format: "$#.00;($#.00)"
  }

# aggregated measures
  measure: revenue {
    type:  sum
    sql: ${account_revenue};;
    value_format: "$#.00;($#.00)"
    filters: {
      field: has_revenue
      value: "yes"
    }
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
    filters: {
      field: has_retained
      value: "yes"
    }
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
    filters: {
      field: has_new
      value: "yes"
    }
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
    filters: {
      field: has_expansion
      value: "yes"
    }
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
    filters: {
      field: has_resurrected
      value: "yes"
    }
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
    filters: {
      field: has_contraction
      value: "yes"
    }
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
    filters: {
      field: has_churned
      value: "yes"
    }
    drill_fields: [detail*]
    link: {
      label: "Explore Worst Churn by account"
      url: "{{ link }}&sorts=SF_account_revenue.account_churned+asc"
    }
  }
  measure: num_of_churned {
    type: count_distinct
    sql:  CASE WHEN ${account_churned} != 0
          THEN ${account}
          ELSE NULL
          END ;;
    filters: {
      field: has_churned
      value: "yes"
    }
    drill_fields: [detail*]
  }

  measure: num_of_churned_negative {
    type: number
    sql: (-1)*${num_of_churned} ;;
    link: {
      label: "Unfortunately the drill-in for churn currently shows all records "
      url: "{{ link }}&sorts=SF_account_revenue.account_churned+asc"
    }
    drill_fields: [detail*]
  }

  measure: num_of_contracted {
    type: count_distinct
    sql:  CASE WHEN ${account_contraction} != 0
          THEN ${account}
          ELSE NULL
          END ;;
    filters: {
      field: has_contraction
      value: "yes"
    }
    drill_fields: [detail*]
  }
  measure: num_of_retained {
    type: count_distinct
    # number of accounts retained is calculated differently than retained revenue
    # if we don't specify contraction and expansion = 0, there will be overlaps
    sql:  CASE WHEN ${account_retained} != 0 and (${account_contraction}=0 and ${account_expansion}=0)
          THEN ${account}
          ELSE NULL
          END ;;
    filters: {
      field: has_retained
      value: "yes"
    }
    drill_fields: [detail*]
  }
  measure: num_of_expanded {
    type: count_distinct
    sql:  CASE WHEN ${account_expansion} != 0
          THEN ${account}
          ELSE NULL
          END ;;
    filters: {
      field: has_expansion
      value: "yes"
    }
    drill_fields: [detail*]
  }
  measure: num_of_resurrected {
    type: count_distinct
    sql:  CASE WHEN ${account_resurrected} != 0
          THEN ${account}
          ELSE NULL
          END ;;
    filters: {
      field: has_resurrected
      value: "yes"
    }
    drill_fields: [detail*]
  }
  measure: num_of_new {
    type: count_distinct
    sql:  CASE WHEN ${account_new} != 0
          THEN ${account}
          ELSE NULL
          END ;;
    filters: {
      field: has_new
      value: "yes"
    }
    drill_fields: [detail*]
  }
  measure: count {
    type: count_distinct
    sql: CASE WHEN ${account_revenue} > 0
          THEN ${account}
          ELSE NULL
          END ;;
    filters: {
      field: has_revenue
      value: "yes"
    }
    drill_fields: [detail*]
  }

  dimension: revenue_tier {
    type: string
    sql: ${TABLE}.revenue_tier ;;
  }

  set: detail {
    fields: [
      account,
      payment_date,
      parent_accountid,
      company_stripe_id,
      name,
      customer_type,
      churn_date,
      resurrected_date,
      parent_customer_type,
      revenue_tier,
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
