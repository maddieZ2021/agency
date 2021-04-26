view: dedup {
derived_table: {
  sql: select distinct
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
                where b.row_number = 1 )
 ;;
}

measure: count {
  type: count
  drill_fields: [detail*]
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
    id,
    ge__c,
    name,
    type_of_customer__c,
    churn_date__c_time,
    resurrected_date__c_time
  ]
}
}
