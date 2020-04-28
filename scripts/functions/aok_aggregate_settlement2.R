# remove grouper for ease -------------------------------------------------
# aok_previous<- read.csv("inputs/2020_01//REACH_SSD_AoK_CleanedDataset_December2019.csv")

aggregate_aok_by_settlement<- function(clean_aok_data,aok_previous, current_month){


  clean_aok_data2<-clean_aok_data
  colnames(clean_aok_data2)<-colnames(clean_aok_data2) %>%
    butteR::remove_kobo_grouper(max_prefix_length = 3) %>%
    butteR::remove_kobo_grouper(max_prefix_length = 3)
  colname_table<-data.frame(with_group=colnames(clean_aok_data), without_group=colnames(clean_aok_data2))


  # extract select multiple and replace 0s and 1s with no's and yes's -------

  sm_cols_df<-butteR::extract_sm_option_columns(df = clean_aok_data2,name_vector=colnames(clean_aok_data2))

  #SELECTING LOGICAL ONLY WILL REMOVE CONCATENATED STRING SELECT MULTIPLE
  df_just_sm<-clean_aok_data2 %>% select(sm_cols_df$sm_options) %>% select_if(is.integer)

  #REPLACE 1S WITH YES 0 WITH NO SM
  df_just_sm<-df_just_sm %>%
    purrr::map_df(function(x)as.character(x) %>%
                    stringr::str_replace_all(c("0"="no", "1"="yes"))) %>% data.frame()

  clean_aok_data2[,colnames(df_just_sm)]<-df_just_sm

  clean_aok_data3 <- clean_aok_data2
  colnames(clean_aok_data3)<-colname_table$with_group

  d.f<-clean_aok_data3

  d.f$D.info_county <-d.f$D.info_county %>% trimws()
  d.f$D.info_settlement_final<-d.f$D.info_settlement_final %>% trimws()
  # d.f$original.D.info_settlement<-d.f$D.info_settlement %>% trimws()
  d.f$D.info_settlement<-d.f$D.info_settlement %>% trimws()


  #ALL NA'S should be fixed - Renk forgot to map Moldooch so we will cut it
  d.f<-d.f %>% filter(is.na(D.info_settlement_other))

  new_columns_added_from_last_round <-colnames(d.f)[colnames(d.f) %in% colnames(aok_previous) ==FALSE]

  ## Let's remove columns we don't need. Notes first, no data in those!!
  d.f <- select(d.f, everything(), -contains("note"), - contains("_other"))




  essential_cols<-c("D.info_state", "D.info_county", "D.info_settlement")

  # APPLY AOK YES FUNCTION --------------------------------------------------



  yes_analysis_added_2020_02<-c("L.food_coping_livelihoods.sell_more_charcoak","U.market_now", "U.market_now_barriers.no_changes",
                                "U.market_now_barriers.communual_violence", "U.market_now_barriers.robberies",
                                "U.market_now_barriers.market_too_far", "U.market_now_barriers.sexual_violence",
                                "U.market_now_barriers.bad_roads", "U.market_now_barriers.flooding_on_way",
                                "U.market_now_barriers_at.no_changes",
                                "I.health_now.ngo_mobile", "I.facility_stocked", "I.community_healthworkers",
                                "M.education_level.secondary_three_four", "M.education_level.primary_six_eight",
                                "M.education_level.primary_one_five", "M.education_level.secondary_one_two",
                                "Q.cash_voucher", "Q.ha_protection_issues_women", "Q.ha_protection_issues_men")
  yes_analysis_switched_from_mode_2020_02<-c("G.food_now_type.Meat", "G.food_now_type.Vegetables",
                                             "G.food_now_type.Main_staples", "G.food_now_type.Fruit", "G.food_now_type.Pulses",
                                             "G.food_now_type.dontknow", "G.food_now_type.Milk_Dairy", "L.current_activities.poultry",
                                             "L.current_activities.none", "L.current_activities.Livestock",
                                             "L.current_activities.hunting", "L.current_activities.Salaries",
                                             "L.current_activities.Crops_for_cash", "L.current_activities.Remittances",
                                             "L.current_activities.Casual_labour", "L.current_activities.other",
                                             "L.current_activities.fishing", "L.current_activities.dontknow",
                                             "L.current_activities.Crops_for_sustenance", "L.current_activities.market")





  aok_yes_cols<-c("D.info_state", "D.info_county", "D.info_settlement", "F.hc_now",
                  "F.idp_now", "F.hcdisp_now", "F.f1.hc_leave", "F.refugee_return_now",
                  "G.food_wild_now", "G.food_wild_emergency", "G.food_distr", "L.livestock_disease",
                  "K.water_source_animals", "K.water_safety", "K.latrine_now",
                  "J.shelter_open_yn", "J.shelter_damage", "J.nfi_distribution",
                  "N.prot_incidence", "N.prot_looting", "N.prot_unaccompanied",
                  "N.land_disputes", "O.mines", "P.supp_feeding", "G.food_coping_comsumption.none",
                  "G.food_coping_comsumption.reduce_meals", "G.food_coping_comsumption.limit_meal_size",
                  "G.food_coping_comsumption.only_children_eat", "G.food_coping_comsumption.dontknow",
                  "G.food_coping_comsumption.other", "G.food_coping_comsumption.skip_days",
                  "G.food_coping_comsumption.less_expensive_food", "G.food_coping_comsumption.other",
                  "J.shelter_materials.ps", "J.shelter_materials.none", "J.shelter_materials.rope",
                  "J.shelter_materials.mud", "J.shelter_materials.pole", "J.shelter_materials.other",
                  "J.shelter_materials.dontknow", "J.shelter_materials.grass",
                  "J.shelter_materials.timber", "L.food_coping_livelihoods.displacement_camp",
                  "L.food_coping_livelihoods.gather_firewood", "L.food_coping_livelihoods.household_begs",
                  "L.food_coping_livelihoods.hunting", "L.food_coping_livelihoods.borrow_money",
                  "L.food_coping_livelihoods.fishing", "L.food_coping_livelihoods.none",
                  "L.food_coping_livelihoods.gather_wild_food", "L.food_coping_livelihoods.other",
                  "L.food_coping_livelihoods.send_children_to_neighbors", "L.food_coping_livelihoods.sell_livestock",
                  "L.food_coping_livelihoods.borrow_food", "L.food_coping_livelihoods.cattle_camps",
                  "L.food_coping_livelihoods.consume_seeds", "L.food_coping_livelihoods.dontknow",
                  "L.food_coping_livelihoods.sell_home_assets", "L.food_coping_livelihoods.slaughter_livestock",
                  "O.mine_areas.school", "O.mine_areas.water_point", "O.mine_areas.medical",
                  "O.mine_areas.firewood", "O.mine_areas.crop", "O.mine_areas.dontknow",
                  "O.mine_areas.graze", "O.mine_areas.other", "O.mine_areas.roads",
                  "O.mine_areas.river", "O.mine_areas.housing", "O.mine_areas.other",
                  "P.children_food_prep_change.not_changed", "P.children_food_prep_change.wild_foods",
                  "P.children_food_prep_change.diet_part", "P.children_food_prep_change.dontknow",
                  "P.children_food_prep_change.reduced_meals_number", "P.children_food_prep_change.other",
                  "U.market_now_barriers.items_expensive", "U.market_now_barriers.no_means_payment",
                  "U.market_now_barriers.live_far", "U.market_now_barriers.marketplace_damage",
                  "U.market_now_barriers.not_available", "U.market_now_barriers.quality_items_notsufficent",
                  "U.market_now_barriers.other", "U.market_now_barriers.safety",
                  "U.market_now_barriers.flooding", "U.market_relevance", "P.breastfeeding_change.not_changed",
                  "P.breastfeeding_change.longer", "P.breastfeeding_change.powder_milk",
                  "P.breastfeeding_change.other", "P.breastfeeding_change.agri_work",
                  "P.breastfeeding_change.dontknow", "P.breastfeeding_change.stopped",
                  "P.breastfeeding_change.due_coping", "P.breastfeeding_change.animal_milk",
                  "P.children_food_prep_change.solid_infant_meals_early", "P.children_food_prep_change.solid_infant_meals_late",
                  "U.market_now_barriers.marketplace_flooded", "U.market_now_barriers.none",
                  "U.market_now_barriers.no_food_water", "U.market_now_barriers.no_road",
                  "U.market_now_barriers.no_shelter", "L.current_activities.charcoal_making",
                  "P.children_food_prep_change.same_meal", "L.current_activities.bee_keeping",
                  "J.shelter_materials.iron_sheets", "J.shelter_flooding", "F.f3.idp_returnees",
                  "L.crop_distruptions", "L.cattle_disease", "I.health_now.none",
                  "I.health_now.herbalist", "I.health_now.pharmarcy", "I.health_now.phc",
                  "I.health_now.hospital", "I.health_now.maternity_center", "I.health_now.other",
                  "I.health_now.dontknow", "I.health_now.nutrition_center", "I.health_now.ngo_clinic",
                  "K.water_quality", "Q.assistance_now", "Q.most_relevant", "Q.ha_satisfied",
                  "Q.complaint_awareness", "Q.ha_protection_concern", "U.market_safety","L.current_activities.pole_selling",
                  "L.current_activities.livestock", "L.current_activities.remittances",
                  "L.current_activities.casual_labour", "L.current_activities.crops_for_sustenance",
                  "L.current_activities.alcohol_brewing","U.market_now_barriers.robbery",
                  "U.market_now_barriers.SGBV", "U.market_now_barriers.conflict", "U.market_now_barriers_at.marketplace_flooded",
                  "U.market_now_barriers_at.traders_not_sell", "U.market_now_barriers_at.none",
                  "U.market_now_barriers_at.marketplace_damaged", "U.market_now_barriers_at.items_quality_low",
                  "U.market_now_barriers_at.some_items_too_expensive", "U.market_now_barriers_at.other",
                  "U.market_now_barriers_at.dontknow", "U.market_now_barriers_at.harassment",
                  "U.market_now_barriers_at.some_items_not_available", yes_analysis_added_2020_02,yes_analysis_switched_from_mode_2020_02)





  aok_yes_cols<-aok_yes_cols[aok_yes_cols %in% colnames(d.f)]
  aok_yes_cols<-aok_yes_cols[aok_yes_cols %in% essential_cols==FALSE]
  aok_yes_cols_dropped<-aok_yes_cols[aok_yes_cols %in% colnames(d.f)==FALSE]


  settlement_yes <- d.f %>%
    select(essential_cols,aok_yes_cols
    ) %>%
    group_by_(.dots = c("D.info_state", "D.info_county", "D.info_settlement")) %>%
    summarise_all(funs(aok_yes))

  ## Let us apply the "aok_no" function to these set of columns


  aok_no_cols<-c("D.info_state", "D.info_county", "D.info_settlement", "G.food_now",
                 "L.land_available", "L.ag_seeds", "L.ag_tools", "R.idp_supported",
                 "R.idp_leadership", "R.local_authorities")

  aok_no_cols<-aok_no_cols[aok_no_cols %in% colnames(d.f)]
  aok_no_cols<-aok_no_cols[aok_no_cols %in% essential_cols==FALSE]
  aok_no_cols_dropped<-aok_no_cols[aok_no_cols %in% colnames(d.f)==FALSE]

  settlement_no <- d.f %>%
    select(essential_cols, aok_no_cols ) %>%
    group_by_(.dots = c("D.info_state", "D.info_county", "D.info_settlement")) %>% ## .dots basically accepts the column values for the function. In this function, we are saying group by the three columns listed!
    summarise_all(funs(aok_no))

  ## Let us apply the "aok_mode" function to these set of columns

  mode_cols_added_2020_02<-c("U.market_now_barriers_at", "Q.ha_type",
                             "Q.ha_mechanism_settlement", "Q.ha_fear_women","Q.ha_fear_men", "I.health_team")
  # as.character(quote(A.base, D.info_state))
  # as.character(expression(A.base, D.info_state)) %>% dput()
  mode_cols_new_spellings_added_2020_03<-c("G.food_now_type.meat", "G.food_now_type.vegetables", "G.food_now_type.main_staples",
                                 "G.food_now_type.fruit", "G.food_now_type.pulses", "G.food_now_type.dontknow",
                                 "G.food_now_type.milk_dairy", "L.current_activities.salaries")
  aok_mode_cols<-c("A.base", "D.info_state", "D.info_county", "D.info_settlement",
                   "F.f1.hc_remain_perc", "F.f2.idp_source", "F.f2.idp_source_state",
                   "F.f2.idp_source_county", "F.f2.idp_perc", "F.f3.returnee_time_arrived",
                   "G.food_source", "G.food_wild_proportion", "G.meals_number",
                   "U.cerial_price_increase", "L.cattle_possession", "L.cattle_access",
                   "K.water_boreholes", "K.water_boreholes_functional", "K.water_now_how",
                   "K.water_now_time", "K.water_source_seasonal", "K.latrine_usage",
                   "K.latrine_no_usage", "K.hand_washing", "J.j2.idp_location",
                   "J.j1.hc_shelter_type1", "J.j1.hc_location", "J.j2.idp_shelter_type1",
                   "J.shelter_open", "J.shelter_destroyed_percent", "J.nfi_need1",
                   "N.prot_now", "N.prot_men_concern", "N.prot_women_concern", "N.prot_girls_concern",
                   "N.prot_boys_concern", "N.comm_relations", "S.shock_hunger",
                   "S.shock_arrival", "S.shock_cerial_price", "S.shock_health",
                   "S.shock_protection", "I.health_now", "I.health_dist", "I.health_no_reason1",
                   "I.health_problems1", "H.death_cause1", "H.mortality_increase",
                   "M.edu_now", "M.edu_no_reason", "M.edu_girl_attendance", "M.edu_boy_attendance",
                   "M.attendance_no_reason1_boys", "M.attendance_no_reason1_girls",
                   "R.community_leadership",
                   "R.info_source_who", "F.f4.refugee_return_source", "F.f4.refugee_return_source_country",
                   "T.ebola_message", "T.main_info_source", "T.who_provided", "T.seek_treatment",
                   "P.malnourished_children", "P.malnourished_elderly", "U.market_now_time",
                   "U.market_now", "U.market_where_food", "U.cereal_market_county",
                   "U.cereal_market_sett", "U.market_where_nfi", "U.nfi_market_county",
                   "U.nfi_market_sett", "F.f1.people_leaving", "F.f2.idp_push",
                   "F.f2.idp_pull", "F.f3.idp_sate_returns", "F.f3.idp_county",
                   "F.f3.returnee_time_arrived", "U.cereal_market_type", "U.nfi_market_type",
                   "F.f1.hc_leave_time", "J.j3.returnee_shelter_type1", "J.j3.returnee_location",
                   "Q.ha_mechanism", "Q.main_language", "Q.sec_language", "Q.language_humanitarians",
                   "Q.language_pref_spoken", "Q.language_pref_written", "Q.language_pref_spoken",
                   "Q.language_pref_written", "R.phone_use", "F.f3.idps_returnees_push",
                   "F.f3.idp_returnees_pull", "F.f4.refugee_returnee_push", "F.f4.refugee_returnees_pull",
                   "L.harvest_perception", "L.harvest_worse", "B.disp_status", "B.interviewed_before",
                   "B.interviewed_last_month", "Q.most_needed_ha", "Q.not_satisfied_reason",
                   "Q.ha_type_acquire", "Q.ha_inkind", "Q.ha_cash", "Q.ha_voucher",
                   "Q.ha_fear", "P.malnourished_children_prop","P.malnourished_children_comp", mode_cols_added_2020_02,
                   mode_cols_new_spellings_added_2020_03)



  aok_mode_cols<-aok_mode_cols[aok_mode_cols %in% colnames(d.f)]
  aok_mode_cols<-aok_mode_cols[aok_mode_cols %in% essential_cols==FALSE]
  aok_mode_cols_dropped<-aok_mode_cols[aok_mode_cols %in% colnames(d.f)==FALSE]

  settlement_equal <- d.f %>%
    select(essential_cols,aok_mode_cols) %>%
    group_by_(.dots = c("D.info_state", "D.info_county", "D.info_settlement")) %>% ## .dots basically accepts the column values for the function. In this function, we are saying group by the three columns listed!
    summarise_all(funs(AoK))



  ## Let us apply the "aok_recent" function to these set of columns
  aok_recent_cols<-c("D.info_state", "D.info_county", "D.info_settlement", "F.f2.idp_time_arrive",
                     "F.f4.refugee_return_time_arrive")

  aok_recent_cols<-aok_recent_cols[aok_recent_cols %in% colnames(d.f)]
  aok_recent_cols<-aok_recent_cols[aok_recent_cols %in% essential_cols==FALSE]
  aok_recent_cols_dropped<-aok_recent_cols[aok_recent_cols %in% colnames(aok_recent_cols)==FALSE]

  settlement_recent <- d.f %>%
    select(essential_cols,aok_recent_cols) %>%
    group_by_(.dots = c("D.info_state", "D.info_county", "D.info_settlement")) %>% ## .dots basically accepts the column values for the function. In this function, we are saying group by the three columns listed!
    summarise_all(funs(aok_recent))

  ## Let us apply the "aok_highest" function to these set of columns

  aok_highest_cols<-c("O.mine_acc_numb")
  aok_highest_cols<-aok_highest_cols[aok_highest_cols%in%colnames(d.f)]
  aok_highest_cols<-aok_highest_cols[aok_highest_cols%in%essential_cols==FALSE]
  aok_highest_cols_dropped<-aok_highest_cols[aok_highest_cols%in%colnames(d.f)==FALSE]

  settlement_highest <- d.f %>%
    select(essential_cols, aok_highest_cols) %>%
    group_by_(.dots = c("D.info_state", "D.info_county", "D.info_settlement")) %>%
    summarise_all(funs(aok_highest))

  ## Let us apply the "aok_conflict" function to these set of columns
  aok_conflict_cols<-c("G.food_no_reason1")
  aok_conflict_cols<-aok_conflict_cols[aok_conflict_cols%in%colnames(d.f)]
  aok_conflict_cols<-aok_conflict_cols[aok_conflict_cols%in%essential_cols==FALSE]
  aok_conflict_cols_dropped<-aok_conflict_cols[aok_highest_cols%in%colnames(d.f)==FALSE]


  settlement_conflict <- d.f %>%
    select(essential_cols, aok_conflict_cols) %>%
    group_by_(.dots = c("D.info_state", "D.info_county", "D.info_settlement")) %>% ## .dots basically accepts the column values for the function. In this function, we are saying group by the three columns listed!
    summarise_all(funs(aok_conflict))


  #table join. Let us merge all these columns/fields into one database
  analysis_df_list<-list(settlement_yes, settlement_no,settlement_equal,settlement_recent, settlement_highest,settlement_conflict)
  # settlement_joined<-purrr::reduce(analysis_df_list, left_join(by= c("D.info_state","D.info_county", "D.info_settlement")))
  settlement_joined<-purrr::reduce(analysis_df_list, left_join)


  settlement<-settlement_joined
  # #Let us rearrange the columns in our database in the same order appear on the tool
  settlement <- settlement %>% select(order(match(names(settlement), names(d.f))))

  #check missing column names

  missing_columns<-colnames(d.f)[colnames(d.f)%in% colnames(settlement)==FALSE]

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

  if(length(check_these>0)){
    print("WARNING you missed these: ")
    check_these %>% dput()
  }


  # ## Now we want to clean the dataset for some skiplogic errors and some NAs that have appeared after our settlement
  ## aggregation has finished. This looks long, but it's very simple! Then we can save it, print it, and BAM! Done.

  #interviewed last month
  settlement$B.interviewed_last_month [settlement$B.interviewed_before != "yes"] <- "SL"


  #Population in settlement--------

  ## local community in ${info_settlement_final}
  # TESTING! ----------------------------------------------------------------
  settlement$F.f1.hc_remain_perc[settlement$F.hc_now != "yes"] <- "SL"
  settlement$F.f1.hc_leave[settlement$F.hc_now != "yes"] <- "SL"
  settlement$F.f1.hc_leave_time[settlement$F.f1.hc_leave != "yes" | settlement$F.hc_now != "yes"] <- "SL"
  settlement$F.f1.people_leaving[settlement$F.f1.hc_leave != "yes" | settlement$F.hc_now != "yes"] <- "SL"




  ## IDPs living in ${info_settlement_final}


  # settlement$F.f2.idp_perc[settlement$F.idp_now != "yes"] <- "SL"
  # settlement$F.f2.idp_push[settlement$F.idp_now != "yes"] <- "SL"
  # settlement$F.f2.idp_pull[settlement$F.idp_now != "yes"] <- "SL"
  # settlement$F.f2.idp_time_arrive[settlement$F.idp_now != "yes"] <- "SL"
  # settlement$F.f2.idp_source[settlement$F.idp_now != "yes"] <- "SL"
  # settlement$F.f2.idp_source_state[settlement$F.idp_now != "yes"] <- "SL"
  # settlement<- settlement %>%
  #   mutate(F.f2.idp_source_county= case_when(F.idp_now!="yes"~"SL",
  #                                            F.f2.idp_source!="yes"~"SL",
  #                                            TRUE~F.f2.idp_source),
  #          F.f2.idp_source_state= case_when(F.idp_now!="yes"~"SL",
  #                                           F.f2.idp_source!="yes"~"SL",
  #                                           TRUE~F.f2.idp_source_state)
  #          )



#TESTING DONE
  # settlement$F.f2.idp_source_county[settlement$F.idp_now != "yes" & settlement$F.f2.idp_source!="yes"] <- "SL"

  ## former IDPs that have returned to ${info_settlement_final}
  # settlement$F.f3.idp_returnees[settlement$F.hcdisp_now != "yes"] <- "SL"
  # settlement$F.f3.idp_sate_returns[settlement$F.hcdisp_now != "yes" |settlement$F.f3.idp_returnees != "yes"] <- "SL"
  # settlement$F.f3.idp_county[settlement$F.hcdisp_now != "yes" |settlement$F.f3.idp_returnees != "yes"] <- "SL"
  # settlement$F.f3.returnee_time_arrived[settlement$F.hcdisp_now != "yes"] <- "SL"
  # settlement$F.f3.idps_returnees_push[settlement$F.hcdisp_now != "yes"] <- "SL"
  # settlement$F.f3.idp_returnees_pull[settlement$F.hcdisp_now != "yes"] <- "SL"
  #
  #
  # #former Refugee that have returned to ${info_settlement_final}
  # settlement$F.f4.refugee_return_source[settlement$F.refugee_return_now != "yes"] <- "SL"
  # settlement$F.f4.refugee_return_source_country[settlement$F.f4.refugee_return_source != "yes" | settlement$F.refugee_return_now != "yes"] <- "SL"
  # settlement$F.f4.refugee_return_time_arrive[settlement$F.refugee_return_now!= "yes"] <- "SL"
  # settlement$F.f4.refugee_returnee_push[settlement$F.refugee_return_now != "yes"] <- "SL"
  # settlement$F.f4.refugee_returnees_pull[settlement$F.refugee_return_now != "yes"] <- "SL"
  #
  # #Food Security----------
  #
  # settlement$G.food_wild_proportion[settlement$G.food_wild_now != "yes"] <- "SL"
  # settlement$G.food_wild_proportion[settlement$G.food_wild_now != "yes"] <- "SL"




  # #Livelihoods------------
  # settlement$L.harvest_worse[settlement$L.harvest_perception != "worse"] <- "SL"
  #
  #
  # #Markets-------------
  #
  # settlement$U.market_where_food[settlement$U.market_now != "yes"] <- "SL"
  # settlement$U.cereal_market_county[settlement$U.market_now != "yes" ] <- "SL"
  # settlement$U.cereal_market_sett[settlement$U.market_now != "yes" ] <- "SL"
  #
  # settlement$U.market_now_time [settlement$U.market_now != "yes"] <- "SL"
  #
  # settlement$U.market_where_nfi[settlement$U.market_now != "yes"] <- "SL"
  # settlement$U.nfi_market_county [settlement$U.market_now != "yes"] <- "SL"
  # settlement$U.nfi_market_sett [settlement$U.market_now != "yes"] <- "SL"
  #
  #
  # U.market_now_barriers_at_cols<-c(settlement %>%ungroup() %>%  select(starts_with("U.market_now_barriers_at")) %>% colnames(),"U.market_now")
  #
  # market_component_df<-settlement[,U.market_now_barriers_at_cols]
  # market_component_df<-purrr::map_df(market_component_df, function(x)ifelse(market_component_df$U.market_now!="yes","SL",x))
  # settlement[,colnames(market_component_df)]<-market_component_df



  #Health----------

  # settlement$I.health_dist[settlement$I.health_now == "none"] <- "SL"
  # settlement$I.facility_stocked[settlement$I.health_now.phc != "yes"| settlement$I.health_now.pharmarcy == "yes"|settlement$I.health_now.ngo_clinic != "yes"| settlement$I.health_now.maternity_center == "yes" ] <- "SL"


  #Ebola-------------

  # settlement$T.ebola_message[(settlement$A.base != "yambio" & settlement$A.base != "maridi" & settlement$A.base != "juba") | (settlement$B.interviewed_last_month != "no" & settlement$B.interviewed_before != "no")] <- "SL"
  # settlement$T.main_info_source[((settlement$T.ebola_message != "yes") & (settlement$A.base != "yambio" & settlement$A.base != "maridi" & settlement$A.base != "juba") | (settlement$B.interviewed_last_month != "no" & settlement$B.interviewed_before != "no"))] <- "SL"
  # settlement$T.who_provided[((settlement$T.ebola_message != "yes") & (settlement$A.base != "yambio" & settlement$A.base != "maridi" & settlement$A.base != "juba") | (settlement$B.interviewed_last_month != "no" & settlement$B.interviewed_before != "no"))] <- "SL"
  # settlement$T.seek_treatment[(settlement$A.base != "yambio" & settlement$A.base != "maridi" & settlement$A.base != "juba") | (settlement$B.interviewed_last_month != "no" & settlement$B.interviewed_before != "no")] <- "SL"






  #Protection-------------

  # settlement$N.comm_relations[settlement$F.idp_now != "yes" |settlement$F.hcdisp_now != "yes"] <- "SL"

  #Shocks--------
  # settlement$S.shock_hunger[settlement$G.food_now != "no"] <- "SL"
  # settlement$S.shock_arrival[settlement$F.idp_now != "yes" | settlement$F.hcdisp_now != "yes"] <- "SL"
  # settlement$S.shock_protection[settlement$N.prot_incidence != "yes" | settlement$N.prot_looting != "yes"] <- "SL"

  #Shelter------------

  ## No HC (shelter)

  # settlement$J.j1.hc_shelter_type1[settlement$F.hc_now != "yes"] <- "SL"
  # settlement$J.j1.hc_location[settlement$F.hc_now != "yes"] <- "SL"

  ## No IDP (shelter)

  # settlement$J.j2.idp_shelter_type1[settlement$F.idp_now != "yes"] <- "SL"
  # settlement$JC.j2.idp_location[settlement$F.idp_now != "yes"] <- "SL"


  #returnees shelter info
  # settlement$J.j3.returnee_shelter_type1[settlement$F.hcdisp_now != "yes"] <- "SL"
  # settlement$J.j3.returnee_location[settlement$F.hcdisp_now != "yes"] <- "SL"



  # settlement$J.shelter_open_yn[settlement$F.idp_now != "yes"] <- "SL"
  # settlement$J.shelter_open[settlement$J.shelter_open_yn != "yes"] <- "SL"


  ## No shelter damage
  # settlement$J.shelter_destroyed_percent[settlement$J.shelter_damage != "yes"] <- "SL"


  # settlement$J.nfi_need1[settlement$F.idp_now != "yes"] <- "SL"

  #Water and Sanitation----------
  ## No boreholes
  # settlement$K.water_boreholes_functional[settlement$K.water_boreholes != "yes"] <- "SL"

  ## Latrine usage
  # settlement$K.latrine_usage[settlement$K.latrine_now != "yes"] <- "SL"
  # settlement$K.latrine_no_usage[settlement$K.latrine_usage == "more_half" | settlement$K.latrine_usage == "dontknow" | settlement$K.latrine_usage == "NC"| settlement$K.latrine_usage == ""] <- "SL"

  #Education------

  ## Education skip logics

  # settlement$M.education_level.primary_one_five[settlement$M.edu_now != "yes"] <- "SL"
  # settlement$M.education_level.primary_six_eight[settlement$M.edu_now != "yes"] <- "SL"
  # settlement$M.education_level.secondary_one_two[settlement$M.edu_now != "yes"] <- "SL"
  # settlement$M.education_level.secondary_three_four[settlement$M.edu_now != "yes"] <- "SL"

  # settlement$M.edu_no_reason[settlement$M.edu_now != "no"] <- "SL"
  # settlement<-settlement %>%
  #   mutate(M.edu_no_reason=if_else(M.edu_now!="no","SL",M.edu_no_reason))
  #
  # settlement$M.edu_boy_attendance[settlement$M.edu_now != "yes"] <- "SL"
  # settlement$M.attendance_no_reason1_girls[settlement$M.edu_now != "yes" | settlement$M.edu_girl_attendance == "more_half"] <- "SL"
  # settlement$M.attendance_no_reason1_boys[settlement$M.edu_now != "yes" | settlement$M.edu_boy_attendance == "more_half" ] <- "SL"
  # settlement$M.edu_girl_attendance[settlement$M.edu_now != "yes"] <- "SL"



  #CCCM-------
  ## No CCCM
  # settlement$R.idp_supported[settlement$F.idp_now != "yes"] <- "SL"
  # settlement$R.local_authorities[settlement$F.idp_now != "yes"] <- "SL"
  # settlement$R.idp_leadership[settlement$F.idp_now != "yes" | settlement$R.community_leadership != "yes"] <- "SL"


  #Mines-------
  ## No mines/UXOs
  # settlement$O.mine_areas[settlement$O.mines != "yes"] <- "SL"
  # settlement$O.mine_areas.housing[settlement$O.mines != "yes"] <- "SL"
  # settlement$O.mine_areas.school[settlement$O.mines != "yes"] <- "SL"
  # settlement$O.mine_areas.medical[settlement$O.mines != "yes"] <- "SL"
  # settlement$O.mine_areas.water_point[settlement$O.mines != "yes"] <- "SL"
  # settlement$O.mine_areas.river[settlement$O.mines != "yes"] <- "SL"
  # settlement$O.mine_areas.crop[settlement$O.mines != "yes"] <- "SL"
  #
  # settlement$O.mine_areas.graze[settlement$O.mines != "yes"] <- "SL"
  # settlement$O.mine_areas.firewood[settlement$O.mines != "yes"] <- "SL"
  # settlement$O.mine_areas.roads[settlement$O.mines != "yes"] <- "SL"
  # settlement$O.mine_areas.other[settlement$O.mines != "yes"] <- "SL"
  # settlement$O.mine_areas.dontknow[settlement$O.mines != "yes"] <- "SL"
  #
  # settlement$O.mine_acc_numb[settlement$O.mines != "yes"] <- "SL"



  #AAP-----------

  # settlement$Q.most_relevant [settlement$Q.assistance_now != "yes"] <- "SL"
  # settlement$Q.ha_satisfied [settlement$Q.assistance_now != "yes"] <- "SL"
  # settlement$Q.not_satisfied_reason [(settlement$Q.ha_satisfied != "no") | (settlement$Q.assistance_now != "yes")] <- "SL"
  # settlement$Q.cash_voucher[(settlement$D.info_state != "uppernile")] <- "SL"
  # settlement$Q.ha_inkind [settlement$Q.ha_type != "in_kind"] <- "SL"
  # settlement$Q.ha_cash [settlement$Q.ha_type != "cash"] <- "SL"
  # settlement$Q.ha_voucher [settlement$Q.ha_type != "voucher"] <- "SL"
  # settlement$Q.complaint_awareness [settlement$Q.assistance_now != "yes"] <- "SL"
  # settlement$Q.ha_mechanism_settlement[settlement$Q.assistance_now != "yes"] <- "SL"
  # settlement$Q.ha_mechanism_settlement[settlement$Q.assistance_now != "yes"] <- "SL"
  # settlement$Q.ha_protection_issues_women[settlement$Q.assistance_now != "yes"] <- "SL"
  # settlement$Q.ha_protection_issues_men[settlement$Q.assistance_now != "yes"] <- "SL"
  # settlement$Q.ha_fear_women [(settlement$Q.assistance_now != "yes") | (settlement$Q.ha_protection_issues_women != "yes")] <- "SL"
  # settlement$Q.ha_fear_men [(settlement$Q.assistance_now != "yes") | (settlement$Q.ha_protection_issues_men != "yes")] <- "SL"



  # settlement[settlement == ""] <- ""

  ## Counting KI coverage per village!

  ki_coverage <- d.f %>%
    select(c(D.info_state, D.info_county, D.info_settlement, B.disp_status)) %>%
    group_by_(.dots = c("D.info_state", "D.info_county", "D.info_settlement")) %>%
    summarise(length(B.disp_status))

  ## Combining settlement and county name for when we join to ArcGIS!! Also adding in the column for KI coverage

  settlement <- settlement %>%
    ungroup() %>%
    mutate(D.settlecounty = paste0(D.info_settlement, D.info_county)) %>%
    mutate(D.ki_coverage = as.numeric(ki_coverage$`length(B.disp_status)`)) %>%
    select(D.info_state, D.info_county, D.info_settlement, D.settlecounty, D.ki_coverage, everything())

  # settlement<- rename(settlement,G.market_now = U.market_now)


  ## Adding KI coverage and month columns

  settlement <- tibble::add_column(as.data.frame(settlement), month = rep(current_month, len = nrow(settlement)), .before = 1)
  return(settlement)}


## Writing the files, we are done!

