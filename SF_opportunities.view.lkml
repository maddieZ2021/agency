view: SF_opportunities {
  derived_table: {
    sql: select * from (SELECT *, row_number() over (partition by id order by lastmodifieddate desc) a FROM `pogon-155405.salesforce_to_bigquery.Opportunity` ) b
      where a = 1
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: hubspot_deal_id__c {
    type: string
    sql: ${TABLE}.hubspot_deal_id__c ;;
  }

  dimension: fiscalquarter {
    type: number
    sql: ${TABLE}.fiscalquarter ;;
  }

  dimension: fiscal {
    type: string
    sql: ${TABLE}.fiscal ;;
  }

  dimension: full_pricing_monthly_fee_amount__c {
    type: number
    sql: ${TABLE}.full_pricing_monthly_fee_amount__c ;;
  }

  dimension: forecastcategory {
    type: string
    sql: ${TABLE}.forecastcategory ;;
  }

  dimension: pilot_pricing_of_ad_spend__c {
    type: number
    sql: ${TABLE}.pilot_pricing_of_ad_spend__c ;;
  }

  dimension: lastmodifiedbyid {
    type: string
    sql: ${TABLE}.lastmodifiedbyid ;;
  }

  dimension: probability {
    type: number
    sql: ${TABLE}.probability ;;
  }

  dimension_group: lastactivitydate {
    type: time
    sql: ${TABLE}.lastactivitydate ;;
  }

  dimension: annual_projected_booked_amount__c {
    type: number
    sql: ${TABLE}.annual_projected_booked_amount__c ;;
  }

  dimension: iswon {
    type: string
    sql: ${TABLE}.iswon ;;
  }

  dimension: hasoverduetask {
    type: string
    sql: ${TABLE}.hasoverduetask ;;
  }

  dimension: discovery_completed__c {
    type: string
    sql: ${TABLE}.discovery_completed__c ;;
  }

  dimension: first_payment_amount__c {
    type: number
    sql: ${TABLE}.first_payment_amount__c ;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: pricing_type__c {
    type: string
    sql: ${TABLE}.pricing_type__c ;;
  }

  dimension: createdbyid {
    type: string
    sql: ${TABLE}.createdbyid ;;
  }

  dimension_group: lastmodifieddate {
    type: time
    sql: ${TABLE}.lastmodifieddate ;;
  }

  dimension: geo_code1__c {
    type: string
    sql: ${TABLE}.geo_code1__c ;;
  }

  dimension: nextstep {
    type: string
    sql: ${TABLE}.nextstep ;;
  }

  dimension_group: pilot_plan_expiry_date__c {
    type: time
    sql: ${TABLE}.pilot_plan_expiry_date__c ;;
  }

  dimension: last_payment_amount__c {
    type: number
    sql: ${TABLE}.last_payment_amount__c ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: _sdc_table_version {
    type: number
    sql: ${TABLE}._sdc_table_version ;;
  }

  dimension: pilot_pricing_monthly_fee_amount__c {
    type: number
    sql: ${TABLE}.pilot_pricing_monthly_fee_amount__c ;;
  }

  dimension: fiscalyear {
    type: number
    sql: ${TABLE}.fiscalyear ;;
  }

  dimension_group: lastvieweddate {
    type: time
    sql: ${TABLE}.lastvieweddate ;;
  }

  dimension: hasopportunitylineitem {
    type: string
    sql: ${TABLE}.hasopportunitylineitem ;;
  }

  dimension: accountid {
    type: string
    sql: ${TABLE}.accountid ;;
  }

  dimension: pricing_plan__c {
    type: string
    sql: ${TABLE}.pricing_plan__c ;;
  }

  dimension: ownerid {
    type: string
    sql: ${TABLE}.ownerid ;;
  }

  dimension: isclosed {
    type: string
    sql: ${TABLE}.isclosed ;;
  }

  dimension_group: _sdc_received_at {
    type: time
    sql: ${TABLE}._sdc_received_at ;;
  }

  dimension_group: closedate {
    type: time
    sql: ${TABLE}.closedate ;;
  }

  dimension: _sdc_sequence {
    type: number
    sql: ${TABLE}._sdc_sequence ;;
  }

  dimension: hasopenactivity {
    type: string
    sql: ${TABLE}.hasopenactivity ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension: budget_confirmed__c {
    type: string
    sql: ${TABLE}.budget_confirmed__c ;;
  }

  dimension_group: lastreferenceddate {
    type: time
    sql: ${TABLE}.lastreferenceddate ;;
  }

  dimension: leadsource {
    type: string
    sql: ${TABLE}.leadsource ;;
  }

  dimension_group: systemmodstamp {
    type: time
    sql: ${TABLE}.systemmodstamp ;;
  }

  dimension: full_pricing_of_ad_spend__c {
    type: number
    sql: ${TABLE}.full_pricing_of_ad_spend__c ;;
  }

  dimension: isdsp__c {
    type: string
    sql: ${TABLE}.isdsp__c ;;
  }

  dimension: booked_amount__c {
    type: number
    sql: ${TABLE}.booked_amount__c ;;
  }

  dimension_group: _sdc_batched_at {
    type: time
    sql: ${TABLE}._sdc_batched_at ;;
  }

  dimension: stagename {
    type: string
    sql: ${TABLE}.stagename ;;
  }

  dimension_group: _sdc_extracted_at {
    type: time
    sql: ${TABLE}._sdc_extracted_at ;;
  }

  dimension: forecastcategoryname {
    type: string
    sql: ${TABLE}.forecastcategoryname ;;
  }

  dimension_group: createddate {
    type: time
    sql: ${TABLE}.createddate ;;
  }

  dimension: loss_reason__c {
    type: string
    sql: ${TABLE}.loss_reason__c ;;
  }

  dimension: description {
    type: string
    sql: ${TABLE}.description ;;
  }

  dimension: geo_code__c {
    type: string
    sql: ${TABLE}.geo_code__c ;;
  }

  dimension: isdeleted {
    type: string
    sql: ${TABLE}.isdeleted ;;
  }

  dimension: roi_analysis_completed__c {
    type: string
    sql: ${TABLE}.roi_analysis_completed__c ;;
  }

  dimension: of_payments__c {
    type: number
    sql: ${TABLE}.of_payments__c ;;
  }

  dimension: campaignid {
    type: string
    sql: ${TABLE}.campaignid ;;
  }

  dimension: activitymetricid {
    type: string
    sql: ${TABLE}.activitymetricid ;;
  }

  dimension: activitymetricrollupid {
    type: string
    sql: ${TABLE}.activitymetricrollupid ;;
  }

  dimension: affectlayer__affectlayer_notes__c {
    type: string
    sql: ${TABLE}.affectlayer__affectlayer_notes__c ;;
  }

  dimension: last_payment_amt__c {
    type: number
    sql: ${TABLE}.last_payment_amt__c ;;
  }

  dimension: first_payment_amt__c {
    type: number
    sql: ${TABLE}.first_payment_amt__c ;;
  }

  dimension: loss_reason_notes__c {
    type: string
    sql: ${TABLE}.loss_reason_notes__c ;;
  }

  dimension_group: dsp_expected_go_live_date__c {
    type: time
    sql: ${TABLE}.dsp_expected_go_live_date__c ;;
  }

  dimension: dsp_expected_go_live_date__c__st {
    type: string
    sql: ${TABLE}.dsp_expected_go_live_date__c__st ;;
  }

  dimension: vertical__c {
    type: string
    sql: ${TABLE}.vertical__c ;;
  }

  dimension: category__c {
    type: string
    sql: ${TABLE}.category__c ;;
  }

  dimension: etailinsights__data_prior_to_close_etailinsights__c {
    type: string
    sql: ${TABLE}.etailinsights__data_prior_to_close_etailinsights__c ;;
  }

  dimension: converted_from_lead__c {
    type: string
    sql: ${TABLE}.converted_from_lead__c ;;
  }

  dimension: opportunity_id_18_digit__c {
    type: string
    sql: ${TABLE}.opportunity_id_18_digit__c ;;
  }

  dimension: pricebook2id {
    type: string
    sql: ${TABLE}.pricebook2id ;;
  }

  dimension_group: contract_end_date__c {
    type: time
    sql: ${TABLE}.contract_end_date__c ;;
  }

  dimension: contract_term__c {
    type: string
    sql: ${TABLE}.contract_term__c ;;
  }

  dimension_group: contract_start_date__c {
    type: time
    sql: ${TABLE}.contract_start_date__c ;;
  }

  dimension: contract_cycle__c {
    type: string
    sql: ${TABLE}.contract_cycle__c ;;
  }

  dimension: auto_renewal__c {
    type: string
    sql: ${TABLE}.auto_renewal__c ;;
  }

  dimension: x60_day_cancellation_notice__c {
    type: string
    sql: ${TABLE}.x60_day_cancellation_notice__c ;;
  }

  dimension: recordtypeid {
    type: string
    sql: ${TABLE}.recordtypeid ;;
  }

  dimension: contract_period__c {
    type: string
    sql: ${TABLE}.contract_period__c ;;
  }

  dimension: service_month_s__c {
    type: number
    sql: ${TABLE}.service_month_s__c ;;
  }

  dimension: contract_months__c {
    type: number
    sql: ${TABLE}.contract_months__c ;;
  }

  dimension: contact__c {
    type: string
    sql: ${TABLE}.contact__c ;;
  }

  dimension: customer_success_owner__c {
    type: string
    sql: ${TABLE}.customer_success_owner__c ;;
  }

  dimension: expectedrevenue {
    type: number
    sql: ${TABLE}.expectedrevenue ;;
  }

  dimension: dsp_service_type__c {
    type: string
    sql: ${TABLE}.dsp_service_type__c ;;
  }

  dimension: requested_amount__c {
    type: number
    sql: ${TABLE}.requested_amount__c ;;
  }

  dimension: funded_amount__c {
    type: number
    sql: ${TABLE}.funded_amount__c ;;
  }

  dimension: perpetua_fee__c {
    type: number
    sql: ${TABLE}.perpetua_fee__c ;;
  }

  dimension: opportunity_type__c {
    type: string
    sql: ${TABLE}.opportunity_type__c ;;
  }

  dimension: owner_role__c {
    type: string
    sql: ${TABLE}.owner_role__c ;;
  }

  dimension: incumbent_software__c {
    type: string
    sql: ${TABLE}.incumbent_software__c ;;
  }

  dimension_group: lock_in_date__c {
    type: time
    sql: ${TABLE}.lock_in_date__c ;;
  }

  dimension: lock_in_period__c {
    type: string
    sql: ${TABLE}.lock_in_period__c ;;
  }

  dimension: data_services__c {
    type: string
    sql: ${TABLE}.data_services__c ;;
  }

  dimension: base_fee__c {
    type: number
    sql: ${TABLE}.base_fee__c ;;
  }

  dimension: ongoing_fee__c {
    type: number
    sql: ${TABLE}.ongoing_fee__c ;;
  }

  dimension: dsp_monthly_budget__c {
    type: number
    sql: ${TABLE}.dsp_monthly_budget__c ;;
  }

  dimension: informed_brady__c {
    type: string
    sql: ${TABLE}.informed_brady__c ;;
  }

  dimension_group: pilot_pricing_expiry_date__c {
    type: time
    sql: ${TABLE}.pilot_pricing_expiry_date__c ;;
  }

  dimension: dspcs_owner__c {
    type: string
    sql: ${TABLE}.dspcs_owner__c ;;
  }

  dimension: subscription_id__c {
    type: string
    sql: ${TABLE}.subscription_id__c ;;
  }

  dimension: belongs_to__c {
    type: string
    sql: ${TABLE}.belongs_to__c ;;
  }

  dimension: a {
    type: number
    sql: ${TABLE}.a ;;
  }




## new
  dimension: is_lost {
    type: yesno
    sql: ${isclosed} AND NOT ${iswon} ;;
  }

  #  - dimension: probability_group
  #    sql_case:
  #      'Won': ${probability} = 100
  #      'Above 80%': ${probability} > 80
  #      '60 - 80%': ${probability} > 60
  #      '40 - 60%': ${probability} > 40
  #      '20 - 40%': ${probability} > 20
  #      'Under 20%': ${probability} > 0
  #      'Lost': ${probability} = 0

  dimension: created_raw {
    type:  date_raw
    sql: ${TABLE}.created_date ;;
  }

  dimension: close_raw {
    type:  date_raw
    sql: ${TABLE}.close_date ;;
  }

  dimension: close_quarter {
    type: date_quarter
    sql: ${TABLE}.close_date ;;
  }

  dimension: days_open {
    type: number
    sql: datediff(days, ${created_raw}, coalesce(${close_raw}, current_date) ) ;;
  }

  dimension: created_to_closed_in_60 {
    hidden: yes
    type: yesno
    sql: ${days_open} <=60 AND ${isclosed} = 'yes' AND ${iswon} = 'yes' ;;
  }

  # measures #

  measure: total_revenue {
    type: sum
    sql: ${amount} ;;
    value_format: "$#,##0"
  }

  measure: average_revenue_won {
    label: "Average Revenue (Closed/Won)"
    type: average
    sql: ${amount} ;;

    filters: {
      field: iswon
      value: "Yes"
    }

    value_format: "$#,##0"
  }

  measure: average_revenue_lost {
    label: "Average Revenue (Closed/Lost)"
    type: average
    sql: ${amount} ;;

    filters: {
      field: is_lost
      value: "Yes"
    }

    value_format: "$#,##0"
  }

  measure: total_pipeline_revenue {
    type: sum
    sql: ${amount} ;;

    filters: {
      field: isclosed
      value: "No"
    }

    value_format: "[>=1000000]0.00,,\"M\";[>=1000]0.00,\"K\";$0.00"
  }

  measure: average_deal_size {
    type: average
    sql: ${amount} ;;
    value_format: "$#,##0"
  }

  measure: count_won {
    type: count

    filters: {
      field: iswon
      value: "Yes"
    }

    drill_fields: [sf__opportunity.id, sf__account.id]
  }

  measure: average_days_open {
    type: average
    sql: ${days_open} ;;
  }

  measure: count_closed {
    type: count

    filters: {
      field: isclosed
      value: "Yes"
    }
  }

  measure: count_open {
    type: count

    filters: {
      field: isclosed
      value: "No"
    }
  }

  measure: count_lost {
    type: count

    filters: {
      field: isclosed
      value: "Yes"
    }

    filters: {
      field: iswon
      value: "No"
    }

    drill_fields: [sf__opportunity.id, sd__account.id]
  }

  measure: win_percentage {
    type: number
    sql: 100.00 * ${count_won} / NULLIF(${count_closed}, 0) ;;
    value_format: "#0.00\%"
  }

  measure: open_percentage {
    type: number
    sql: 100.00 * ${count_open} / NULLIF(${count}, 0) ;;
    value_format: "#0.00\%"
  }


  set: detail {
    fields: [
      hubspot_deal_id__c,
      fiscalquarter,
      fiscal,
      full_pricing_monthly_fee_amount__c,
      forecastcategory,
      pilot_pricing_of_ad_spend__c,
      lastmodifiedbyid,
      probability,
      lastactivitydate_time,
      annual_projected_booked_amount__c,
      iswon,
      hasoverduetask,
      discovery_completed__c,
      first_payment_amount__c,
      id,
      pricing_type__c,
      createdbyid,
      lastmodifieddate_time,
      geo_code1__c,
      nextstep,
      pilot_plan_expiry_date__c_time,
      last_payment_amount__c,
      name,
      _sdc_table_version,
      pilot_pricing_monthly_fee_amount__c,
      fiscalyear,
      lastvieweddate_time,
      hasopportunitylineitem,
      accountid,
      pricing_plan__c,
      ownerid,
      isclosed,
      _sdc_received_at_time,
      closedate_time,
      _sdc_sequence,
      hasopenactivity,
      type,
      amount,
      budget_confirmed__c,
      lastreferenceddate_time,
      leadsource,
      systemmodstamp_time,
      full_pricing_of_ad_spend__c,
      isdsp__c,
      booked_amount__c,
      _sdc_batched_at_time,
      stagename,
      _sdc_extracted_at_time,
      forecastcategoryname,
      createddate_time,
      loss_reason__c,
      description,
      geo_code__c,
      isdeleted,
      roi_analysis_completed__c,
      of_payments__c,
      campaignid,
      activitymetricid,
      activitymetricrollupid,
      affectlayer__affectlayer_notes__c,
      last_payment_amt__c,
      first_payment_amt__c,
      loss_reason_notes__c,
      dsp_expected_go_live_date__c_time,
      dsp_expected_go_live_date__c__st,
      vertical__c,
      category__c,
      etailinsights__data_prior_to_close_etailinsights__c,
      converted_from_lead__c,
      opportunity_id_18_digit__c,
      pricebook2id,
      contract_end_date__c_time,
      contract_term__c,
      contract_start_date__c_time,
      contract_cycle__c,
      auto_renewal__c,
      x60_day_cancellation_notice__c,
      recordtypeid,
      contract_period__c,
      service_month_s__c,
      contract_months__c,
      contact__c,
      customer_success_owner__c,
      expectedrevenue,
      dsp_service_type__c,
      requested_amount__c,
      funded_amount__c,
      perpetua_fee__c,
      opportunity_type__c,
      owner_role__c,
      incumbent_software__c,
      lock_in_date__c_time,
      lock_in_period__c,
      data_services__c,
      base_fee__c,
      ongoing_fee__c,
      dsp_monthly_budget__c,
      informed_brady__c,
      pilot_pricing_expiry_date__c_time,
      dspcs_owner__c,
      subscription_id__c,
      belongs_to__c,
      a
    ]
  }
}
