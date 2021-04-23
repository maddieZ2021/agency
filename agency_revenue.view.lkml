view: agency {
  derived_table:
  { sql: -- stripe to SF invoice
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
        ( select distinct
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
        dau as (
        select
        account__c,
        billing_id,
        stripe_created_invoice_date as payment_date,
        invoice,
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
        on abs.parent_logo__c = dd.id)

      select * from dau
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: account_id {
    type: string
    sql: ${TABLE}.account__c ;;
  }

  dimension: billing_id {
    type: string
    sql: ${TABLE}.billing_id ;;
  }

  dimension_group: date {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.payment_date ;;
  }

  measure: invoice {
    type: number
    sql: ${TABLE}.invoice ;;
    drill_fields: [detail*]
  }

  dimension: description {
    type: string
    sql: ${TABLE}.description__c ;;
  }

  dimension: parent_id {
    type: string
    sql: ${TABLE}.parent_logo__c ;;
  }

  dimension: company_stripe_id {
    type: string
    sql: ${TABLE}.ge__c ;;
  }

  dimension: account_name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: customer_type {
    type: string
    sql: ${TABLE}.type_of_customer__c ;;
  }

  dimension_group: churn_date {
    type: time
    sql: ${TABLE}.churn_date__c ;;
  }

  dimension_group: resurrected_date {
    type: time
    sql: ${TABLE}.resurrected_date__c ;;
  }

  dimension: parent_customer_type {
    type: string
    sql: ${TABLE}.parent_customertype ;;
  }

  dimension: parent_agency_identifier {
    type:  yesno
    sql: ${TABLE}.parent_customertype = 'Agency' ;;
  }

  measure: sum_of_invoice {
    type: sum
    sql:  sql: ${TABLE}.invoice;;
    drill_fields: [detail*]
  }

  set: detail {
    fields: [
      account_id,
      billing_id,
      invoice,
    #  payment_date,
      description,
      parent_id,
      company_stripe_id,
      account_name,
      customer_type,
    #  churn_date,
    #  resurrected_date,
      parent_customer_type
    ]
  }
}
