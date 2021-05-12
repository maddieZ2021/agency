view: geo_company_name {
  derived_table: {
    sql:
          SELECT
              geo_company_id,
              country_code,
              company_name,
              cast(company_id as string) as company_id,
              geo_company_name.customer_billing_status,
              DATE(COALESCE(parent_plan.churned_date, plan.churned_date)) AS churned_date
          FROM `aggregated.geo_company_name` geo_company_name
          -- TODO: Don't query the billing tables directly, add an aggregated table to have this information readily available.
          LEFT JOIN `billing_service.billing_customer` customer USING (company_id)
          LEFT JOIN `billing_service.billing_plan` plan ON plan.customer_id = customer.id
          -- NOTE: We often "merge" companies under the same stripe account and invoice them together. Thus we have a parent
          -- customer id. This means that the billing status of the company is really the billing status of the parent IF the
          -- parent id exists
          LEFT JOIN `billing_service.billing_plan` parent_plan ON parent_plan.customer_id = customer.parent_customer_id

       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: geo_company_id {
    type: string
    sql: ${TABLE}.geo_company_id ;;
  }

  dimension: country_code {
    type: string
    sql: ${TABLE}.country_code ;;
  }

  dimension: company_name {
    type: string
    sql: ${TABLE}.company_name ;;
  }

  dimension: company_id {
    type: string
    sql: ${TABLE}.company_id ;;
  }


  dimension: customer_billing_status {
    type: string
    sql: ${TABLE}.customer_billing_status ;;
  }

  dimension: churned_date {
    type: date
    datatype: date
    sql: ${TABLE}.churned_date ;;
  }

  set: detail {
    fields: [geo_company_id, country_code, company_name, customer_billing_status, churned_date]
  }
}
