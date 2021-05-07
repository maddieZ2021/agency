view: SF_opportunities {
  derived_table: {
    sql: select * from (SELECT *, row_number() over (partition by id order by lastmodifieddate desc) a FROM `pogon-155405.salesforce_to_bigquery.Opportunity` ) b
      where a = 1
       ;;
  }
# These views shouldn't be changed, extend them in sf_extends instead.
# If you need to re-generate the file, simply delete it and click "Create View from Table" and rename it from account to _account (for example).

    dimension: id {
      primary_key: yes
      type: string
      sql: ${TABLE}.id ;;
    }

    # dimension_group: _fivetran_synced {
    #   type: time
    #   timeframes: [
    #     raw,
    #     time,
    #     date,
    #     week,
    #     month,
    #     quarter,
    #     year
    #   ]
    #   sql: ${TABLE}._fivetran_synced ;;
    # }

    dimension: account_id {
      type: string
      # hidden: yes
      sql: ${TABLE}.accountid ;;
    }

    dimension: amount {
      type: number
      sql: ${TABLE}.amount ;;
    }

    dimension: campaign_id {
      type: string
      # hidden: yes
      sql: ${TABLE}.campaignid ;;
    }

    dimension_group: close {
      type: time
      timeframes: [
        raw,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.closedate ;;
    }

    dimension: created_by_id {
      type: string
      sql: ${TABLE}.createdbyid ;;
    }

    dimension_group: created {
      type: time
      timeframes: [
        raw,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.createddate ;;
    }

    # dimension: current_generators_c {
    #   type: string
    #   sql: ${TABLE}.current_generators_c ;;
    # }

    # dimension: delivery_installation_status_c {
    #   type: string
    #   sql: ${TABLE}.delivery_installation_status_c ;;
    # }

    dimension: description {
      type: string
      sql: ${TABLE}.description ;;
    }

    dimension: expected_revenue {
      type: number
      sql: ${TABLE}.expectedrevenue ;;
    }

    dimension: fiscal {
      type: string
      sql: ${TABLE}.fiscal ;;
    }

    dimension: fiscal_quarter {
      type: number
      sql: ${TABLE}.fiscalquarter ;;
    }

    dimension: fiscal_year {
      type: number
      sql: ${TABLE}.fiscalyear ;;
    }

    dimension: forecast_category {
      type: string
      sql: ${TABLE}.forecastcategory ;;
    }

    dimension: forecast_category_name {
      type: string
      sql: ${TABLE}.forecastcategoryname ;;
    }

    dimension: has_open_activity {
      type: yesno
      sql: ${TABLE}.hasopenactivity ;;
    }

    dimension: has_opportunity_line_item {
      type: yesno
      sql: ${TABLE}.hasopportunitylineitem ;;
    }

    dimension: has_overdue_task {
      type: yesno
      sql: ${TABLE}.hasoverduetask ;;
    }

    dimension: is_closed {
      type: yesno
      sql: ${TABLE}.isclosed ;;
    }

    dimension: is_deleted {
      type: yesno
      sql: ${TABLE}.isdeleted ;;
    }

    dimension: is_private {
      type: yesno
      sql: ${TABLE}.isprivate ;;
    }

    dimension: is_won {
      type: yesno
      sql: ${TABLE}.iswon ;;
    }

    dimension_group: last_activity {
      type: time
      timeframes: [
        raw,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.lastactivitydate ;;
    }

    dimension: last_modified_by_id {
      type: string
      sql: ${TABLE}.lastmodifiedbyid ;;
    }

    dimension_group: last_modified {
      type: time
      timeframes: [
        raw,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.lastmodifieddate ;;
    }

    dimension_group: last_referenced {
      type: time
      timeframes: [
        raw,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.lastreferenceddate ;;
    }

    dimension_group: last_viewed {
      type: time
      timeframes: [
        raw,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.lastvieweddate ;;
    }

    dimension: lead_source {
      type: string
      sql: ${TABLE}.leadsource ;;
    }

    # dimension: main_competitors_c {
    #   type: string
    #   sql: ${TABLE}.main_competitors_c ;;
    # }

    dimension: name {
      type: string
      sql: ${TABLE}.name ;;
    }

    dimension: next_step {
      type: string
      sql: ${TABLE}.nextstep ;;
    }

    dimension: order_number_c {
      type: string
      sql: ${TABLE}.ordernumberc ;;
    }

    dimension: owner_id {
      type: string
      sql: ${TABLE}.ownerid ;;
    }

    # dimension: pricebook_2_id {
    #   type: string
    #   sql: ${TABLE}.pricebook_2_id ;;
    # }

    dimension: probability {
      type: number
      sql: ${TABLE}.probability ;;
    }

    dimension: stage_name {
      type: string
      sql: ${TABLE}.stagename ;;
    #  order_by_field: opportunity_stage.sort_order
    }

    # dimension_group: system_modstamp {
    #   type: time
    #   timeframes: [
    #     raw,
    #     time,
    #     date,
    #     week,
    #     month,
    #     quarter,
    #     year
    #   ]
    #   sql: ${TABLE}.system_modstamp ;;
    # }

    # dimension: total_opportunity_quantity {
    #   type: number
    #   sql: ${TABLE}.total_opportunity_quantity ;;
    # }

    dimension: type {
      type: string
      sql: ${TABLE}.type ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    # ----- Sets of fields for drilling ------
    set: detail {
      fields: [
        id,
        stage_name,
        forecast_category_name,
        name,
      ]
    }
  }
