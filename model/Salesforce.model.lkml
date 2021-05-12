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
  join: geo_company_name {
    type:  left_outer
    sql_on: ${SF_account_revenue.company_stripe_id}=${geo_company_name.company_id} ;;
    relationship: many_to_one
  }

}
