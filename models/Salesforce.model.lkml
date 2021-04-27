connection: "perpetua_db"

# include all the views
include: "/*.view"

datagroup: agency_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: agency_default_datagroup

explore: SF_account_revenue {
  label: "SF_account_revenue"
  join: company_billing_status {
    type: left_outer
    relationship: many_to_one
    sql_on: ${SF_account_revenue.company_stripe_id} = ${company_billing_status.company_id} ;;

  }

}
