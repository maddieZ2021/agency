view: abs {
  derived_table: {
    sql: select distinct * from
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
              where rank = 1
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

  set: detail {
    fields: [
      lastmodifieddate_time,
      account__c,
      billing_id,
      invoice,
      stripe_created_invoice_date,
      description__c,
      parent_logo__c,
      rank
    ]
  }
}
