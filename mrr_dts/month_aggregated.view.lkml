view: month_aggregated {
  derived_table: {
    sql:
    select
      date_trunc(dt, month) as month,
      account_id,
      sum(invoice) as invoice
    from ${base.SQL_TABLE_NAME}
    group by 1,2
    having invoice > 0 -- for edge cases id '0011U00000Ouun4QAB' who was charged and refunded on 2019-8-15, so its monthly fee cancelled out
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: month {
    type: date
    datatype: date
    sql: ${TABLE}.month ;;
  }

  dimension: account_id {
    type: string
    sql: ${TABLE}.account_id ;;
  }

  dimension: invoice {
    type: number
    sql: ${TABLE}.invoice ;;
  }

  set: detail {
    fields: [month, account_id, invoice]
  }
}
