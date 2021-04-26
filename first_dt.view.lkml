view: first_dt {
  derived_table: {
        sql:
        select
            account_id,
            min(dt) as first_dt,
            date_trunc(min(dt), week) as first_week,
            date_trunc(min(dt), month) as first_month
        from ${base.SQL_TABLE_NAME}
        group by 1 ;;
        }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: account_id {
    type: string
    sql: ${TABLE}.account_id ;;
  }

  dimension: first_dt {
    type: date
    datatype: date
    sql: ${TABLE}.first_dt ;;
  }

  dimension: first_week {
    type: date
    datatype: date
    sql: ${TABLE}.first_week ;;
  }

  dimension: first_month {
    type: date
    datatype: date
    sql: ${TABLE}.first_month ;;
  }

  set: detail {
    fields: [account_id, first_dt, first_week, first_month]
  }
}
