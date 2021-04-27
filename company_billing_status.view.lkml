view: company_billing_status {
  derived_table: {
    sql: select distinct company_id, customer_billing_status from  `pogon-155405.aggregated.geo_company_name`
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: company_id {
    type: number
    sql: ${TABLE}.company_id ;;
  }

  dimension: customer_billing_status {
    type: string
    sql: ${TABLE}.customer_billing_status ;;
  }

  set: detail {
    fields: [company_id, customer_billing_status]
  }
}
