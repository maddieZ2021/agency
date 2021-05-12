view: geo_company_name {
  sql_table_name: `pogon-155405.aggregated.geo_company_name`
    ;;

  dimension: company_id {
    type: string
    sql: cast(${TABLE}.company_id as string) ;;
  }

  dimension: company_name {
    type: string
    sql: ${TABLE}.company_name ;;
  }

  dimension: country_code {
    type: string
    sql: ${TABLE}.country_code ;;
  }

  dimension: customer_billing_status {
    type: string
    sql: ${TABLE}.customer_billing_status ;;
  }

  dimension: geo_company_id {
    type: string
    sql: ${TABLE}.geo_company_id ;;
  }

  dimension: is_vendor {
    type: yesno
    sql: ${TABLE}.is_vendor ;;
  }

  dimension: suffixless_company_name {
    type: string
    sql: ${TABLE}.suffixless_company_name ;;
  }

  measure: count {
    type: count
    drill_fields: [company_name, suffixless_company_name]
  }
}
