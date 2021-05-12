connection: "perpetua_db"

include: "/*.view.lkml"

explore: cohort {
  join: geo_company_name {
    type:  left_outer
    sql_on: ${cohort.company_stripe_id}=${geo_company_name.company_id} ;;
    relationship: many_to_one
}
}
