connection: "perpetua_db"

# include all the views
include: "/*.view"

datagroup: agency_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: agency_default_datagroup

explore: agency {
  label: "SF_account_revenue"

}
