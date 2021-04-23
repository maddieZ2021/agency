connection: "perpetua_db"

# include all the views
include: "/*.view"
# include: "/views/*.view"

datagroup: agency_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: agency_default_datagroup

explore: MRR {
  from: mrr
}

explore: agency {
  from: agency
}
