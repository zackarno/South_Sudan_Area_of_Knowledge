

aggregate_aok_by_settlement<- function(clean_aok_data, current_month, kobo_survey_sheet){

# kobo_survey_sheet<-ks
# clean_aok_data<-aok_clean4
  select_ones_questions_with_group<-kobo_survey_sheet %>%
    butteR::add_groups_to_xlsform_names() %>%
    filter(str_detect(type,"select_one")) %>%
    pull(name)


  clean_aok_data2<-clean_aok_data
  colnames(clean_aok_data2)<-colnames(clean_aok_data2) %>%
    butteR::remove_kobo_grouper(max_prefix_length = 3) %>%
    butteR::remove_kobo_grouper(max_prefix_length = 3)

  colname_table<-data.frame(with_group=colnames(clean_aok_data),
                            without_group=colnames(clean_aok_data2)) %>%
    mutate_all(.funs = as.character)



  # extract select multiple and replace 0s and 1s with no's and yes's -------

  sm_cols_df<-butteR::extract_sm_option_columns(df = clean_aok_data2,name_vector=colnames(clean_aok_data2))


  # this removes the concatenated select multiple from the table since we dont want to analyze this column
  sm_cols_df2<-sm_cols_df %>%
    filter(parent_name!=sm_options) %>%
    ungroup() %>% as_tibble()
  # sm_cols_df2%>% View()
  sm_concat_cols<-sm_cols_df %>%
    filter(parent_name==sm_options)

  sm_concat_cols2<-sm_concat_cols %>%
    left_join(colname_table, by=c("sm_options"= "without_group")) %>%
    filter(!str_detect(sm_options,"note"))

  sm_cols_df3<-sm_cols_df2 %>%
    left_join(colname_table, by=c("sm_options"= "without_group"))

  colnames(clean_aok_data2)<-colname_table$with_group



  essential_grouping_cols<-c("D.info_state", "D.info_county", "D.info_settlement")

  clean_aok_data2<-clean_aok_data2 %>% mutate_at(.vars = essential_grouping_cols, .funs = trimws)

  clean_up_sm_questions<- function(x) {x %>%as.character() %>%  trimws() %>% tolower() }
  clean_aok_data2<-clean_aok_data2 %>% mutate_at(.vars=sm_cols_df3$with_group, .funs = clean_up_sm_questions)

  ## Let's remove columns we don't need. Notes first, no data in those!!
  clean_aok_data2<- select(clean_aok_data2, everything(), -contains("note"), - contains("_other"))

  # these are select_one exceptions that should be aggregated with AoK yes
  special_case_select_one_to_aggregate_with_aok_yes<-c("F.f1.hc_leave", "F.f3.idp_returnees", "F.hc_now", "F.hcdisp_now",
                                                       "F.idp_now", "F.refugee_return_now", "G.food_distr", "G.food_wild_emergency",
                                                       "G.food_wild_now", "I.community_healthworkers", "I.facility_stocked",
                                                       "J.nfi_distribution", "J.shelter_damage", "J.shelter_flooding",
                                                       "J.shelter_open_yn", "K.latrine_now", "K.water_quality", "K.water_safety",
                                                       "K.water_source_animals", "L.cattle_disease", "L.crop_distruptions",
                                                       "L.livestock_disease", "N.land_disputes", "N.prot_incidence",
                                                       "N.prot_looting", "N.prot_unaccompanied", "O.mines", "P.supp_feeding",
                                                       "Q.assistance_now", "Q.cash_voucher", "Q.complaint_awareness",
                                                       "Q.ha_protection_issues_men", "Q.ha_protection_issues_women",
                                                       "Q.ha_satisfied", "Q.most_relevant", "U.market_now", "U.market_safety")

  select_multiple_aok_yes<- sm_cols_df3$with_group
  aok_yes_cols<-c(special_case_select_one_to_aggregate_with_aok_yes,select_multiple_aok_yes)

  aok_yes_cols<-aok_yes_cols[aok_yes_cols %in% colnames(clean_aok_data2)]
  aok_yes_cols<-aok_yes_cols[!aok_yes_cols %in% essential_grouping_cols]
  aok_yes_cols_dropped<-aok_yes_cols[aok_yes_cols %in% colnames(clean_aok_data2)==FALSE]

  ## Let us apply the "aok_no" function to these set of columns
  aok_no_cols<-c( "G.food_now",
                  "L.land_available", "L.ag_seeds", "L.ag_tools", "R.idp_supported",
                  "R.idp_leadership", "R.local_authorities")

  aok_highest_cols<-c("O.mine_acc_numb")

  aok_recent_cols<-c("F.f2.idp_time_arrive",
                     "F.f4.refugee_return_time_arrive")
  aok_conflict_cols<-c("G.food_no_reason1")

  aok_yes_cols<-aok_yes_cols[!aok_yes_cols%in%c(aok_no_cols,aok_highest_cols,aok_recent_cols,aok_conflict_cols,sm_concat_cols2$with_group)]

  select_ones_questions_with_group<-c(select_ones_questions_with_group,sm_concat_cols2$with_group)

  aok_mode_cols<-select_ones_questions_with_group[!select_ones_questions_with_group %in%
                                                    c(special_case_select_one_to_aggregate_with_aok_yes,
                                                      c("A.enumerator_id"),
                                                      aok_no_cols,aok_highest_cols, aok_recent_cols,aok_conflict_cols)]





  settlement_yes<-clean_aok_data2 %>%
    select(essential_grouping_cols,aok_yes_cols
    ) %>%
    group_by(!!!syms(essential_grouping_cols)) %>%
    summarise_all(.funs=aok_yes)




  settlement_no <-  clean_aok_data2 %>%
    select(essential_grouping_cols,aok_no_cols) %>%
    group_by(!!!syms(essential_grouping_cols)) %>%
    summarise_all(.funs=aok_no)



  settlement_mode <- clean_aok_data2 %>%ungroup() %>%
    select(essential_grouping_cols,aok_mode_cols) %>%
    group_by(!!!syms(essential_grouping_cols)) %>%
    summarise_all(.funs=AoK)


  settlement_recent <- clean_aok_data2 %>%
    select(essential_grouping_cols,aok_recent_cols) %>%
    group_by(!!!syms(essential_grouping_cols)) %>%
    summarise_all(.funs=aok_recent)


  settlement_highest <-clean_aok_data2 %>%
    select(essential_grouping_cols,aok_highest_cols) %>%
    group_by(!!!syms(essential_grouping_cols)) %>%
    summarise_all(.funs=aok_highest)

  ## Let us apply the "aok_conflict" function to these set of columns




  settlement_conflict <- clean_aok_data2 %>%
    select(essential_grouping_cols, aok_conflict_cols) %>%
    group_by(!!!syms(essential_grouping_cols)) %>%
    summarise_all(.funs=aok_conflict)


  #table join. Let us merge all these columns/fields into one database

  # analysis_df_list<-list(settlement_yes, settlement_no,settlement_mode)
  analysis_df_list<-list(settlement_yes, settlement_no,settlement_mode,settlement_recent, settlement_highest,settlement_conflict)
  settlement_joined<-purrr::reduce(analysis_df_list, right_join, by=c("D.info_state","D.info_county","D.info_settlement"))
settlement_yes$U.market_now_barriers_at
  # all(settlement_yes$D.info_settlement==settlement_no$D.info_settlement)

  # settlement_joined$U.market_now_barriers_at.y

  settlement<-settlement_joined
  # #Let us rearrange the columns in our database in the same order appear on the tool
  settlement <- settlement %>% select(order(match(names(settlement), names(clean_aok_data2))))

  #check missing column names

  missing_columns<-colnames(clean_aok_data2)[colnames(clean_aok_data2)%in% colnames(settlement)==FALSE]


  columns_not_needed<- c("month", "today", "deviceid", "A.type_visit", "B.ki_gender",
                         "B.D_startTime", "D.info_settlement_final", "D.info_payam", "D.type_contact",
                         "D.d1.remotely_how", "D.d1.remotely_how.s_phone", "D.d1.remotely_how.mob_app",
                         "D.d1.remotely_how.m_phone", "D.d1.remotely_how.other", "D.d1.remotely_how.radio",
                         "D.d1.remotely_how.internet", "D.d1.remotely_how.dontknow", "D.ki_recent",
                         "D.F_startTme", "F.f2.other", "F.f2.other_idp", "F.f3.other_returnees_push",
                         "F.f3.other_returnees_pull", "F.f4.other_refugee_returnee_push",
                         "F.f4.other_refugee_returnees_pull", "G.G_startTme_1", "G.food_now_type",
                         "G.food_enough_fewmeals", "G.food_coping_comsumption", "G.P_startTme",
                         "P.breastfeeding_change", "P.children_food_prep_change", "P.L_startTme",
                         "L.food_coping_livelihoods", "L.current_activities", "L.cutlivation_yes_land_no",
                         "L.crops_yes_tools_no", "L.U_startTme", "U.market_now_barriers",
                         "U.market_now_barriers.dontknow", "I.I_startTime", "T.T_startTime",
                         "T.key_meaasage", "T.key_signs", "T.get_ebola", "T.everyone_risk",
                         "T.prevent_ebola", "T.hand_wash", "T.wild_animals", "T.bush_meat",
                         "T.no_fruits", "T.no_sharp_object", "T.protective_clothes", "T.seek_medical",
                         "H.H_startTme", "N.N_startTime", "N.safe_yes_incident_yes", "N.S_startTime",
                         "J.j1.J_startTime", "J.shelter_materials", "K.K_startTime", "M.M_startTme",
                         "R.R_startTme", "O.O_startTme", "O.mine_areas", "Z.Z_startTme",
                         "Z.end_survey_protection", "LE_1_remote", "LE_2_unpopulated",
                         "LE_3_nohostcommunity", "LE_4_food_enough_fewmeals", "LE_5_food_insufficient_manymeals",
                         "LE_7_food_enough_malnutritiondeath", "LE_8_food_enough_hungersevere",
                         "LE_9_cutlivation_yes_land_no", "LE_10_crops_yes_seedstools_no",
                         "LE_11_forage_yes_wildfoods_no", "LE_12_morehalf_wildfood_morethan_3meals",
                         "LE_13_hunting_source_yes_activity_no", "LE_14_fishing_source_yes_activity_no",
                         "LE_15_market_source_yes_access_no", "LE_16_assist_source_yes_no_distribution",
                         "LE_17_safe_but_serious_concerns_women", "LE_18_safe_but_serious_concerns_men",
                         "LE_19_safe_but_serious_concerns_girls", "LE_20_safe_but_serious_concerns_boys",
                         "LE_26_safe_yes_incident_yes", "LE_21_mainshelter_HC_permanent",
                         "LE_22_mainshelter_IDP_permanent", "LE_23_IDP_bush_yes_open_no",
                         "LE_24_water_borehole_no_borehole", "X_id", "X_uuid",
                         "start", "end", "A.enumerator_id", "B.survey_start_time", "B.ki_age",
                         "gps", "Z._gps_latitude", "Z._gps_longitude", "Z._gps_altitude",
                         "X__version__", "X_submission_time","X_index", "X_validation_status")


  check_these<-missing_columns[missing_columns %in% columns_not_needed ==FALSE]


  ki_coverage <- clean_aok_data2 %>%
    group_by(!!!syms(essential_grouping_cols)) %>%
    summarise(D.ki_coverage=n())


  settlement <- settlement %>%
    ungroup() %>%
    left_join(ki_coverage) %>%
    mutate(D.settlecounty=paste0(D.info_settlement,D.info_county)) %>%
    select(D.info_state, D.info_county, D.info_settlement, D.settlecounty, D.ki_coverage, everything())


  settlement <- tibble::add_column(as.data.frame(settlement), month = rep(current_month, len = nrow(settlement)), .before = 1)
  return(settlement)}




