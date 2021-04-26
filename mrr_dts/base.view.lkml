view: base {
  derived_table: {
    sql:
          select
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
           from ${abs.SQL_TABLE_NAME} as abs
          left join ${dedup.SQL_TABLE_NAME} as dedup
          on abs.account__c= dedup.id
          left join dedup as dd
          on abs.parent_logo__c = dd.id
          where dd.type_of_customer__c = 'Agency'
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: lastmodifieddate {
    type: time
    sql: ${TABLE}.lastmodifieddate ;;
  }

  dimension: account__c {
    type: string
    sql: ${TABLE}.account__c ;;
  }

  dimension: billing_id {
    type: string
    sql: ${TABLE}.billing_id ;;
  }

  dimension: invoice {
    type: number
    sql: ${TABLE}.invoice ;;
  }

  dimension: stripe_created_invoice_date {
    type: date
    datatype: date
    sql: ${TABLE}.stripe_created_invoice_date ;;
  }

  dimension: description__c {
    type: string
    sql: ${TABLE}.description__c ;;
  }

  dimension: parent_logo__c {
    type: string
    sql: ${TABLE}.parent_logo__c ;;
  }

  dimension: rank {
    type: number
    sql: ${TABLE}.rank ;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: ge__c {
    type: string
    sql: ${TABLE}.ge__c ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: type_of_customer__c {
    type: string
    sql: ${TABLE}.type_of_customer__c ;;
  }

  dimension_group: churn_date__c {
    type: time
    sql: ${TABLE}.churn_date__c ;;
  }

  dimension_group: resurrected_date__c {
    type: time
    sql: ${TABLE}.resurrected_date__c ;;
  }


  set: detail {
    fields: [
      lastmodifieddate_time,
      account__c,
      billing_id,
      invoice,
      stripe_created_invoice_date,
      description__c,
      parent_logo__c,
      rank,
      id,
      ge__c,
      name,
      type_of_customer__c,
      churn_date__c_time,
      resurrected_date__c_time
    ]
  }
}
