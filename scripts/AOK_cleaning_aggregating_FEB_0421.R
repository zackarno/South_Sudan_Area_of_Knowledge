library(tidyverse)
library(butteR)
library(koboloadeR)
library(lubridate)
library(sf)

source("scripts/functions/aok_aggregation_functions.R")
source("scripts/functions/aok_cleaning_functions.R")
source("scripts/functions/aok_aggregate_settlement2.R")

admin_gdb<- "../../gis_data/gis_base/boundaries/county_shapefile"
latest_settlement_data <- "inputs2/2020_02/SSD_Settlements_V38.csv"
iso_date<- Sys.Date() %>%  str_replace_all("-","_")

payams<-sf::st_read("inputs2/gis_data/boundaries", "ssd_bnd_adm3_wfpge_un")
master_settlement<-read.csv(latest_settlement_data, stringsAsFactors = FALSE)
colnames(master_settlement)<-paste0("mast.",colnames(master_settlement))
master_settlement_sf<- st_as_sf(master_settlement,coords=c("mast.X","mast.Y"), crs=4326)
month_of_assessment<-"2020-02-01"


# LOAD RAW DATA -----------------------------------------------------------
# aok_raw<-download_aok_data(keys_file = "scripts/functions/keys.R")

# write.csv(aok_raw,"inputs/2020_02/2020_02_FEB_AOK_RAW_20200301.csv")
# aok_raw<-read.csv("inputs/2020_02/2020_02_FEB_AOK_RAW_20200301.csv", stringsAsFactors = F,na.strings = c("n/a","", ""))
# aok_raw<-read.csv("inputs/2020_02/2020_02_FEB_AOK_RAW_20200304_from_KOBO.csv", stringsAsFactors = F,na.strings = c("n/a","", ""))
# aok_raw %>% nrow()
# LOAD AOK RAW DATA
aok_raw_list <-butteR::read_all_csvs_in_folder(input_csv_folder = "inputs2/2020_02/raw_data/from_kobo")
purrr::map(aok_raw_list,nrow)

aok_raw<- dplyr::bind_rows(aok_raw_list)


#making sure all our uuid in the dataset are unique

#quick check that all settlements are in
aok_raw <- distinct(aok_raw,X_uuid, .keep_all= TRUE)


sm_cols_df<-butteR::extract_sm_option_columns(df = aok_raw,name_vector=colnames(aok_raw))

fix_logical_to_yn<-function(x){
  x %>% as.character() %>% str_replace_all(c("1"="yes","0"="no"))
}

aok_raw<-aok_raw %>%
  mutate_at(.vars= sm_cols_df$sm_options,.funs = fix_logical_to_yn)


aok_raw<-aok_raw %>%
  mutate(name_county_low= paste0(D.info_settlement,D.info_county) %>% butteR::tolower_rm_special())

master_settlement_sf<-master_settlement_sf %>%
  mutate(name_county_low= mast.NAMECOUNTY %>% butteR::tolower_rm_special())

#early check to make sure all settlement in list are in master
aok_raw%>%
  mutate(name_county_low= paste0(D.info_settlement, D.info_county) %>% butteR::tolower_rm_special()) %>%
  filter(D.info_settlement!="other") %>%
  filter(!name_county_low%in%master_settlement_sf$name_county_low) %>% as_tibble() %>%
  select(D.info_county,D.info_settlement, name_county_low)


master_settlement_sf
# STEP 1 COMPILE CLEANING LOGS --------------------------------------------
cleaning_log_path <- "inputs2/2020_02/cleaning_logs"



cleaning_logs<-butteR::read_all_csvs_in_folder(input_csv_folder = cleaning_log_path)

# cleaning_logs$REACH_SSD_AoK_Pariang_Cleaning_Log_Feb2020.csv %>% View()


cleaning_log<-bind_rows(cleaning_logs)

cleaning_log<-cleaning_log %>%
  mutate(uuid=ifelse(is.na(uuid),ï..uuid, uuid)) %>%
  select(-ï..uuid)

cleaning_log<- cleaning_log %>% left_join(aok_raw %>%
                                            select(X_uuid,A.base ), by=c("uuid"="X_uuid"))

cleaning_log_list<-list()
cleaning_log_list[["main"]]<-cleaning_log
# cleaning_log %>%
  # filter(str_detect(indicator,"settlement")) %>% View()

cleaning_log_actionable<- cleaning_log %>% filter(change_type!="no_action")
cleaning_log_actionable<- cleaning_log_actionable %>%
  mutate(new_value=ifelse(is.na(new_value),"response_blanked", new_value)) %>% as_tibble()
cleaning_log_actionable %>% filter(new_value=="response_blanked")
# CHECK CLEANING LOG
butteR::check_cleaning_log(df = aok_raw, df_uuid = "X_uuid",
                           cl = cleaning_log_actionable,
                           cl_change_type_col = "change_type",
                           cl_change_col = "indicator",
                           cl_uuid = "uuid",
                           cl_new_val = "new_value")
aok_clean<-butteR::implement_cleaning_log(df = aok_raw, df_uuid = "X_uuid",
                               cl = cleaning_log_actionable,
                               cl_change_type_col = "change_type",
                               cl_change_col = "indicator",
                               cl_uuid = "uuid",
                               cl_new_val = "new_value")
aok_raw %>% nrow();  aok_clean %>% nrow(); aok_raw %>% nrow() - aok_clean %>% nrow()

aok_raw %>%
  filter(!is.na(D.info_settlement_other)) %>%
  select(D.info_settlement_other)




# OUTPUT ISSUES TO AOS

# IMPLEMENT CLEANING LOG

# CREATE AOK_CLEAN
# STEP 2 NEW SETTLEMENTS --------------------------------------------------
# PERHAPS THE MOST DIFFICULT STEP
# OUTPUTS: NEW ITEMSET, NEW MASTER SETTLEMENT FILE, CLEANING LOG

new_settlements<-butteR::read_all_csvs_in_folder(input_csv_folder = "inputs2/2020_02/new_settlements")
new_settlements$new_settlements_test.csv
new_sett<-bind_rows(new_settlements)



new_sett<-new_sett %>%
  mutate(uuid=ï..uuid) %>%
  filter(action=="Map") %>%
    select(-ï..uuid)

new_sett$D.info_settlement_other

sett_aok_id_key<-cleaning_log_actionable %>%
  filter(str_detect(indicator, "settlement")) %>%
  select(uuid, indicator,Current_value, new_value) %>%
  filter(Current_value=="other")
sett_aok_id_key<- aok_raw %>%
  inner_join(sett_aok_id_key, by= c("X_uuid"="uuid")) %>%
  select(old_value=D.info_settlement_other, new_value)

new_sett_with_key<-new_sett %>%
  left_join(sett_aok_id_key,
            by= c("D.info_settlement_other"="old_value"))


new_sett2<- new_sett_with_key %>%
  mutate(name_new_settlement= ifelse(!is.na(new_value), new_value, D.info_settlement_other)) %>%
  select(-D.info_settlement_other)


adm2<- st_read(admin_gdb,"ssd_admbnda_adm2_imwg_nbs_20180401" , stringsAsFactors = F)
adm2<-st_transform(adm2,crs=4326)
new_sett_sf<-st_as_sf(new_sett2,coords=c("long","lat"), crs=4326)


ggplot()+geom_sf(data=adm2)+
  geom_sf_label(data= new_sett_sf,aes(label =name_new_settlement))+
  geom_sf(data=new_sett_sf)

nrow(new_sett)



# SPATIAL JOIN
new_sett_sf<-new_sett_sf %>% st_join( adm2 %>% dplyr::select(adm2=admin2RefN))

new_sett_sf<-new_sett_sf %>%
  mutate(
    new.enum_sett_county=paste0(name_new_settlement,D.info_county) %>% tolower_rm_special(),
    new.adm2_sett_county=paste0(name_new_settlement,adm2) %>% tolower_rm_special()
  )

master_settlement_sf<-master_settlement_sf %>%
  mutate(
    mast.settlement_county_sanitized= mast.NAMECOUNTY %>% tolower_rm_special()
  )



  # CHECK IF NEW SETTLEMENTS HAVE BEEN FIXED IN CL --------------------------
# aok_clean1<-aok_raw
aok_clean
# aok_clean1[aok_clean1$X_uuid=="b4d97108-3f34-415d-9346-f22d2aa719ea","D.info_settlement_other"]<-NA
# aok_clean1[aok_clean1$X_uuid=="b4d97108-3f34-415d-9346-f22d2aa719ea","D.info_settlement"]<-"Bajur"


remove_from_new_sett<-aok_clean %>%
  filter(X_uuid %in% new_sett_sf$uuid  & is.na(D.info_settlement_other))%>%
  select(X_uuid,D.info_settlement) %>% pull(X_uuid)

any(master_settlement$mast.NAME=="Dhor_tuol")
new_sett_sf
aok_other_not_in_settlement_data<-aok_clean %>%
  filter(!is.na(D.info_settlement_other)) %>%
  filter(!X_uuid %in% new_sett_sf$uuid) %>%
  select(X_uuid,A.base,D.info_settlement, D.info_settlement_other) %>%
  arrange(A.base) # %>%
# aok_other_not_in_settlement_data %>% write.csv("FEB2020_AoK_other_settlements_not_CL_or_new_settlements.csv")

#WHY DO THE NUMBERS NOT EXACTLY ADD UP-- BECAUSE THERE ARE DUPLICATES IN THE NEW_SETT_SF -- THIS COUDL BE OK
aok_clean %>% filter(!is.na(D.info_settlement_other)) %>% nrow()-new_sett_sf %>% nrow()
any(new_sett_sf$name_new_settlement %>% duplicated())
new_sett_sf %>%
  group_by(name_new_settlement) %>%
  mutate(number_occurence=n()) %>%
  filter(number_occurence==2)
any(new_sett_sf$D.info_settlement_other %>% duplicated())
any(aok_other_not_in_settlement_data$D.info_settlement_other %>% duplicated())

#I think this should be skipped
# new_sett_sf<- new_sett_sf %>% filter(!uuid %in% remove_from_new_sett)
# new_sett_sf %>% left_join(cleaning_log_actionable$Current_value
# cleaning_log_actionable %>% filter(str_detect(indicator, "settlement")) %>% View()

aok_raw %>% filter(X_uuid == "0e69c900-9c47-4a4c-9178-c30442c8965b") %>% select(D.info_settlement_other)

# NEW SETTLEMENT DATA WHICH MATCHES MASTER SETTLEMENTS EXACTLY ------------
# new_sett_sf %>% View()
exact_matches1<-new_sett_sf %>%
  mutate(matched_where= case_when(new.enum_sett_county %in% master_settlement_sf$mast.settlement_county_sanitized~"enum", #CHECK WITH ENUMS INPUT
                                  new.adm2_sett_county %in% master_settlement_sf$mast.settlement_county_sanitized~"shapefile_only" ),
         county_use= ifelse(matched_where=="shapefile_only",adm2,D.info_county)) %>%
  filter(!is.na(matched_where)) #ONLY RETURN EXACT MATCHES

# WRITE EXACT MATCHES TO CLEANING LOG TO THEN IMPLEMENT ON DATA.

# debugonce(exact_matches_to_cl)
# debugonce(exact_matches_to_cl)
aok_exact_matches_cl<-exact_matches_to_cl(exact_match_data = exact_matches1,
                                          user = "Zack",
                                          uuid_col = "uuid",
                                          settlement_col = "name_new_settlement")


#NOW IMPLEMENT THIS CLEANING LOG!

aok_exact_matches_cl %>%
  filter(!uuid %in% aok_clean$X_uuid)

cleaning_log %>% filter(uuid %in%"eb29c50f-89e0-4c57-8614-cdb9867c0082")
aok_raw %>% filter(X_uuid %in% "eb29c50f-89e0-4c57-8614-cdb9867c0082") %>% select(A.base)
aok_exact_matches_cl_filt<-aok_exact_matches_cl %>%
  filter(uuid %in% aok_clean$X_uuid)
cleaning_log_list[["auto_gen_exact_matches"]]<- aok_exact_matches_cl_filt
aok_exact_matches_cl_filt
# debugonce(butteR::implement_cleaning_log)
aok_clean2<-butteR::implement_cleaning_log(df = aok_clean,df_uuid = "X_uuid",
                                           cl = aok_exact_matches_cl_filt,
                                           cl_change_type_col = "change_type",
                                           cl_change_col = "indicator",
                                           cl_uuid = "uuid",
                                           cl_new_val = "new_value")



#EXTRACT NEW SETTLEMENTS WHICH DO NO MATCH
new_sett_sf_unmatched<- new_sett_sf %>% filter(!uuid %in% exact_matches1$uuid)
new_sett_sf_unmatched %>% nrow()
#REMOVE MATCHED SETTLEMENTS FROM MASTER
master_settlement_sf_not_matched<-master_settlement_sf %>%
  filter(mast.settlement_county_sanitized %in% new_sett_sf_unmatched$new.enum_sett_county==FALSE)

# MATCH NEW SETTLEMENT TO CLOSEST SETTLEMENT IN MASTER --------------------

new_with_closest_old<-butteR::closest_distance_rtree(new_sett_sf_unmatched %>%
                                                       st_as_sf(coords=c("X","Y"), crs=4326) ,master_settlement_sf_not_matched)
# new_with_closest_old$name_new_settlement
#CLEAN UP DATASET
new_with_closest_old_vars<-new_with_closest_old %>%
  mutate(new.D.info_settlement_other= name_new_settlement %>% gsub("-","_",.)) %>%
  select(uuid,
         new.A.base=A.base,
         new.county_enum=D.info_county,
         new.county_adm2= adm2,
         new.sett_county_enum=new.enum_sett_county,
         new.sett_county_adm2= new.adm2_sett_county,
         new.D.info_settlement_other=name_new_settlement,
         mast.settlement=mast.NAMEJOIN,
         mast.settlement_county_sanitized,
         dist_m)



# ADD A FEW USEFUL COLUMNS - THIS COULD BE WRITTEN TO A CSV AND WOULD BE THE BEST OUTPUT TO BE REVIEWED



settlements_best_guess<-new_with_closest_old_vars %>%
  mutate(gte_50=ifelse(dist_m<500, " < 500 m",">= 500 m"),
         string_proxy=stringdist::stringdist(a =new.sett_county_enum,
                                             b= mast.settlement_county_sanitized,
                                             method= "dl", useBytes = TRUE)
  ) %>%
  arrange(dist_m,desc(string_proxy))


# HOWEVER, TO KEEP EVERYTHING IN THE R ENVIRONMENT- HERE IS AN INTERACTIVE FUNCTION TO MODIFY THE SETTLEMENT BEST GUESS DF IN PLACE
# OUTUT WILL BE A CLEANING LOG (IF THERE ARE CHANGES TO BE MADE)
fuzzy_settlement_matching<-c("not_done", "done")[2]
if(fuzzy_settlement_matching=="not_done"){
new_settlement_evaluation<-evaluate_unmatched_settlements(user= "zack",new_settlement_table = settlements_best_guess, uuid_col="uuid")
# write.csv(new_settlement_evaluation$checked_setlements,"outputs/2020_02/cleaning_logs/2020_02_evaluated_settlement_log.csv")
# write.csv(new_settlement_evaluation$cleaning_log,"outputs/2020_02/cleaning_logs/2020_02_evaluated_settlement_cleaning_log.csv")

}

if(fuzzy_settlement_matching=="done"){
  new_settlement_evaluation<-list()
  new_settlement_evaluation$checked_setlements<-read_csv("outputs/2020_02/cleaning_logs/2020_02_evaluated_settlement_log.csv")
  new_settlement_evaluation$cleaning_log<-read_csv("outputs/2020_02/cleaning_logs/2020_02_evaluated_settlement_cleaning_log.csv")

}





cleaning_log_list[["auto_gen_settlements_evaluated"]]<- new_settlement_evaluation$cleaning_log



#this takes care of any new settlements uuid records that were removed in cleaning
new_settlement_evaluation$cleaning_log<- new_settlement_evaluation$cleaning_log %>%
  filter(uuid %in%  aok_clean2$X_uuid)

new_settlement_evaluation$cleaning_log %>% nrow()

aok_clean3<-butteR::implement_cleaning_log(df = aok_clean2,df_uuid = "X_uuid",
                                           cl =new_settlement_evaluation$cleaning_log,
                                           cl_change_type_col = "change_type",
                                           cl_change_col = "suggested_indicator",
                                           cl_uuid = "uuid",
                                           cl_new_val = "suggested_new_value")



# aok_clean3[,c("L.current_activities.crops_for_sustenance","L.current_activities.livestock")]<-aok_clean3[,c("L.current_activities.crops_for_sustenance","L.current_activities.livestock")] %>%
#   purrr:::map_df(function(x) str_replace_all(x,c("yes"="1","no"="0")) %>% as.numeric() %>% as.integer)
# ADD NEW SETTLEMENTS TO NEW ITEMSET  -------------------------------------

#put into itemset format
new_sets_to_add_itemset<-new_settlement_evaluation$checked_setlements %>%
  filter(action==2) %>%
  mutate(
    list_name="settlements",
    label= new.D.info_settlement_other %>% gsub("_","-", .)
  ) %>%
  select(list_name,name=new.D.info_settlement_other, label,admin_2=new.county_adm2)

# read in previous itemset
itemset<-read.csv("inputs/tool/REACH_SSD_AoK_V38_Febuary2020/itemsets.csv", strip.white = T, stringsAsFactors = T,na.strings = c(" ",""))
itemset_not_other<-itemset %>% filter(name!="other")
itemset_other<- itemset %>% filter(name=="other")


itemset_binded<-bind_rows(list(new_sets_to_add_itemset,itemset_not_other)) %>% arrange(admin_2)
itemset_full_binded<- bind_rows(list(itemset_binded,itemset_other))
itemset_file_name<- paste0("2020_02_itemset_updated",iso_date,".csv")
#write to csv for next aok round
# write.csv(itemset_full_binded,paste0("outputs/2020_02/",itemset_file_name))

# NEXT WE ADD THE NEW SETTLEMENTS TO THE SHAPEFILE ------------------------

# add to master file ------------------------------------------------------
# new_sett<-read.csv("inputs/new_settlements/20200207_New_settlement_Jan2020_ZA.csv", stringsAsFactors = FALSE)

# new_sett %>% head()
master_settlement<-read.csv(latest_settlement_data, stringsAsFactors = FALSE)

master_settlement<- master_settlement %>%
  mutate(
    name_county_low= NAMECOUNTY %>% tolower_rm_special()
  )
aok_raw%>%
  mutate(name_county_low= paste0(D.info_settlement, D.info_county) %>% butteR::tolower_rm_special()) %>%
  filter(D.info_settlement!="other") %>%
  filter(!name_county_low%in%master_settlement$name_county_low) %>% as_tibble() %>%
  select(D.info_county,D.info_settlement, name_county_low)

# master_settlement$DATA_SOURC
# new_sett_sf

master_settlement%>%
  mutate(
    aok_sett_id= 1: nrow(master_settlement),
    name_county_low= NAMECOUNTY %>% tolower_rm_special()
  ) %>%
  group_by(name_county_low) %>%
  mutate(num=n()) %>%
  filter(num>1) %>%
  # arrange(name_county_low) %>% View()
  distinct(X,Y, .keep_all = T)

master_settlement_adj<-master_settlement%>%
    mutate(
      aok_sett_id= 1: nrow(master_settlement),
      name_county_low= NAMECOUNTY %>% tolower_rm_special()
    ) %>%
    group_by(name_county_low) %>%
  # filter(SRC_VERIFD==max(SRC_VERIFD, na.rm = T)) %>%
    mutate(
      X_avg= mean(X,na.rm=T),
      Y_avg=mean(Y, na.rm=T)
    ) %>%
  filter(aok_sett_id== max(aok_sett_id)) %>%
  ungroup()

aok_raw%>%
  mutate(name_county_low= paste0(D.info_settlement, D.info_county) %>% butteR::tolower_rm_special()) %>%
  filter(D.info_settlement!="other") %>%
  filter(!name_county_low%in%master_settlement_adj$name_county_low) %>% as_tibble() %>%
  select(D.info_county,D.info_settlement, name_county_low)

master_settlement_adj<- master_settlement_adj %>%
  select(INDEX:name_county_low, X= X_avg, Y=Y_avg)





# distinct(X,Y, .keep_all = T)


new_setts_add_to_master<-new_settlement_evaluation$checked_setlements %>%
  filter(action==2) %>%
  mutate(
    uuid= uuid,
    NAME= new.D.info_settlement_other %>%  gsub("'","",.) %>% gsub(" ","-", .),
    NAMEJOIN= NAME  %>% gsub("-","_",.),
    NAMECOUNTY=paste0(NAMEJOIN,new.county_adm2),
    COUNTYJOIN= new.county_adm2 ,
    DATE= month_of_assessment %>% ymd(),
    DATA_SOURC="AOK",
    IMG_VERIFD= 0
  ) %>% #get coordinates from field data back in
  left_join(new_sett_sf %>%st_drop_geometry_keep_coords() ,
              by=c("uuid"="uuid")) %>%
  select(NAME,NAMEJOIN,NAMECOUNTY,COUNTYJOIN,DATE,DATA_SOURC,IMG_VERIFD,X,Y)

# gsub("[[:punct:]]","",.)
# new_setts_add_to_master %>% View()

master_new<-bind_rows(list(new_setts_add_to_master,master_settlement_adj %>% mutate(DATE=dmy(DATE))))
master_new %>% nrow()
master_settlement_adj %>% nrow()+ nrow(new_setts_add_to_master)


master_new_file_name<- paste0("2020_02_working_master_settlement_list_",iso_date,".csv")
write_outputs_to_csv<-c("yes","no")[2]
if(write_outputs_to_csv=="yes"){
 write.csv(master_new,paste0("outputs/2020_02/settlement_lists/",master_new_file_name))
}
master_new %>% filter(str_detect(NAMEJOIN, "Hai"))
#lets make sure that everything from aok_clean3 is in master_new now
master_new %>% as_tibble()

master_new %>% filter(NAMEJOIN=="Dhor_tuol") %>% select(NAMEJOIN, NAME, NAMECOUNTY , name_county_low)
master_new %>% filter(str_detect(NAME, "Agoro")) %>% select(NAMEJOIN, NAME, NAMECOUNTY , name_county_low)

master_new<- master_new %>%
  mutate(
    name_county_low= NAMECOUNTY %>% tolower_rm_special()
  )
aok_raw%>%
  mutate(name_county_low= paste0(D.info_settlement, D.info_county) %>% butteR::tolower_rm_special()) %>%
  filter(D.info_settlement!="other") %>%
  filter(!name_county_low%in%master_new$name_county_low) %>% as_tibble() %>%
  select(D.info_county,D.info_settlement, name_county_low)



#write to csv v 39


###########################################
#ok assume we have imlpemented all cleaning
############################################
# aok_clean3<-aok_clean2
new_setts_with_no_mapping_uuid<-aok_clean3 %>%
  filter(!is.na(D.info_settlement_other)) %>%
  filter(!X_uuid %in% new_sett_sf$uuid) %>%
  select(X_uuid,A.base,D.info_settlement, D.info_settlement_other) %>%
  pull(X_uuid)

aok_clean4<-aok_clean3 %>%
  filter(!X_uuid %in% new_setts_with_no_mapping_uuid )

aok_clean3 %>% nrow()
aok_clean4 %>% nrow()
aok_clean4 %>% filter(!is.na(D.info_settlement_other)) %>%
  select(D.info_settlement, D.info_settlement_other)

#maybe change the way assessment month is represented

month_label<-month(month_of_assessment,label = T)
aggregated_file_name<- paste0("outputs/2020_02/aggregated_data/",
                              iso_date,
                              "_reach_ssd_settlement_aggregated_AoK_",month_label,"2020_Data.csv")
aggregated_long_term_file_name<- paste0("outputs/2020_02/aggregated_data/",
                              iso_date,
                              "_reach_ssd_settlement_aggregated_LONGTERM_AoK_",month_label,"2020.csv")

#next start with the rmeove grouper stuff.
prev_round<-read.csv("inputs/2020_01/2020_02_13_reach_ssd_aok_clean_data_compiled.csv", stringsAsFactors = FALSE, na.strings=c("", " ", NA, "NA"))
# debugonce(aggregate_aok_by_county)

aok_clean3 %>% nrow()
#good check!
aok_clean4<- aok_clean4%>%
  mutate(name_county_low= paste0(D.info_settlement, D.info_county) %>% butteR::tolower_rm_special())# %>%
  # filter(D.info_settlement!="other") %>%
aok_clean4 %>%   filter(!name_county_low%in%master_new$name_county_low) %>% as_tibble() %>%
  select(D.info_county,D.info_settlement, name_county_low)


# this is because relevant question was changed in cleaning
aok_clean4 %>%
  filter(is.na(M.edu_no_reason )) %>% select(M.edu_now, M.edu_no_reason ) %>%
  filter(M.edu_now=="no")

aok_clean4$name_county_low

# aggregate_aok_by_settlement %>% debugonce()
aok_clean_aggregated_settlement<-aggregate_aok_by_settlement(clean_aok_data = aok_clean4,
                                                             aok_previous = prev_round,
                                                             current_month = month_of_assessment)




kobo_tool<-"inputs2/2020_02/kobo_tool/2020_02_SSD_AoK_tool_38b.xlsx"
kt<-readxl::read_xlsx(path = kobo_tool, sheet = "survey")
# kt<- butteR::add_groups_to_xlsform_names(kt)
kc<-readxl::read_xlsx(path = kobo_tool, sheet = "choices")


colname_table<-data.frame(no_groups=butteR::remove_kobo_grouper(colnames(aok_clean_aggregated_settlement),max_prefix_length = 3) %>% butteR::remove_kobo_grouper(max_prefix_length = 3), with_groups=colnames(aok_clean_aggregated_settlement))
aok_clean_aggregated2<-aok_clean_aggregated_settlement
colnames(aok_clean_aggregated2)<- colname_table$no_groups

kt2<-kt %>%
  filter(name%in% colnames(aok_clean_aggregated2))

settlement_level_kobold<-kobold::kobold(survey = kt2,choices = kc,data = aok_clean_aggregated2)

settlement_level_kobold_SL<-kobold:::relevant_updater(settlement_level_kobold)
#THI NEEDS TO BE REMOVED FROM THE TOOL
#########################
aok_aggregated_3<-settlement_level_kobold_SL$data
colnames(aok_aggregated_3)<- colname_table$with_groups


aok_aggregated_3%>%
  mutate(name_county_low= paste0(D.info_settlement, D.info_county) %>% butteR::tolower_rm_special()) %>%
  # filter(D.info_settlement!="other") %>%
  filter(!name_county_low%in%aok_clean4$name_county_low) %>% as_tibble() %>%
  select(D.info_county,D.info_settlement, name_county_low)

#JOIN PAYAM DATA
aok_aggregated_3<- aok_aggregated_3 %>%
  mutate(
    name_county_low=paste0(D.info_settlement,D.info_county) %>% butteR::tolower_rm_special())

# master_settlements_new<-read_csv("outputs/2020_02/2020_02_master_settlement_list_2020_04_15.csv")


# master_new_2<-master_new %>%
  # distinct(X,Y, .keep_all = T)



  # master_new_2 %>%
#   mutate(
#     name_county_low= NAMECOUNTY %>% butteR::tolower_rm_special()
#     ) %>%
#   group_by(name_county_low) %>%
#   mutate(num=n()) %>%
#   filter(num>1) %>% arrange(name_county_low) %>% write.csv("clear_duplicates_in_master_settlement_list.csv")

aok_aggregated_3<- aok_aggregated_3 %>%
  mutate(
    name_county_low=paste0(D.info_settlement,D.info_county) %>% butteR::tolower_rm_special())

aok_aggregated_3 %>% nrow()
aok_aggregated_4<-aok_aggregated_3 %>% left_join(master_new %>% select(X,Y,name_county_low), by="name_county_low")
?distinct
aok_aggregated_5<- aok_aggregated_4 %>% distinct()

aok_with_payams<-st_as_sf(aok_aggregated_5,coords = c("X","Y"),crs=4326) %>%
  st_join(payams %>% select(adm3_name)) %>%st_drop_geometry()

aok_with_payams<- aok_with_payams %>% rename(G.market_now=U.market_now)

long_term_aok_latest_file<- "inputs2/2020_02/REACH_SSD_AoK_LongTermSettlementData.csv"
previous_long_term<-read.csv(long_term_aok_latest_file,stringsAsFactors = FALSE,
                             row.names = NULL, na.strings = c(""," ",NA, "NA","n/a"),
                             strip.white = TRUE)

previous_long_term$month %>% unique()
colnames(previous_long_term)[!colnames(previous_long_term)%in% colnames(aok_clean_aggregated_settlement)]
colnames(aok_clean_aggregated_settlement)[!colnames(aok_clean_aggregated_settlement)%in% colnames(previous_long_term)]
nrow(previous_long_term)
previous_long_term %>% filter(!is.na(L.ag_inputs)) %>% nrow()
previous_long_term$L.ag_inputs
colnames(previous_long_term)<- colnames(previous_long_term) %>%
  str_replace_all(c("L.current_activities.Crops_for_sustenance"="L.current_activities.crops_for_sustenance",
                    "L.current_activities.Livestock"= "L.current_activities.livestock"))
aok_with_payams$month<- aok_with_payams$month %>% as.character()
long_term_aggregated_new<-bind_rows(previous_long_term, aok_with_payams)
# long_term_aggregated_new %>% filter(!is.na(L.ag_inputs))


# write.csv(aok_with_payams,aggregated_file_name, na="SL")
# write.csv( long_term_aggregated_new,aggregated_long_term_file_name,na = "SL")

aok_feb<- read.csv("outputs/2020_02/aggregated_data/2020_04_21_reach_ssd_settlement_aggregated_AoK_Feb2020_Data.csv", stringsAsFactors = FALSE, na.strings=c(""," ")) %>% select(-X)
aok_feb<-aok_feb %>%
  mutate(month= month %>% ymd())

aok_feb<- aok_feb %>%
  mutate(
    name_county_low=paste0(D.info_settlement,D.info_county) %>% butteR::tolower_rm_special())

mast_settlement<-read.csv("outputs/2020_03/settlement_lists/2020_03_working_master_settlement_list_2020_04_21.csv", stringsAsFactors = FALSE, na.strings=c(""," "))

#good quick check to make sure we have all settlementsin master list
aok_feb %>%
  filter(!name_county_low %in% mast_settlement$name_county_low) %>%
  select(D.info_state,D.info_county, D.info_settlement)

aok_feb %>% nrow()
aok_with_coordinates<-aok_feb %>% left_join(mast_settlement %>% select(X,Y,name_county_low), by="name_county_low")
aok_with_coordinates %>% nrow()
aok_sf<-st_as_sf(aok_with_coordinates,coords = c("X","Y"),crs=4326)

hex_grid <- st_read(dsn = "inputs/GIS",layer ="Grids_info", stringsAsFactors=F ) %>% st_transform(crs=4326) %>% select(-settlement)

# hex_data_poly<-hex_grid %>% st_join(aok_sf)


data_hex_pt<- aok_sf %>% st_join(hex_grid)
data_hex_pt<-data_hex_pt %>% st_join(adm2)

grid_evaluation_table<-data_hex_pt %>%
  group_by(State_id) %>%
  summarise(num_setts_grid=n(),
            num_ki_grid=sum(D.ki_coverage))
valid_grids<-grid_evaluation_table %>%
  filter(num_setts_grid>1,num_ki_grid>1) %>%
  pull(State_id)

#create new composites
data_hex_pt_w_composite<-data_hex_pt %>%
  mutate(
    idp_sites= ifelse(J.j2.idp_location=="informal_sites",1,0),
    IDP_present= ifelse(F.idp_now=="yes",1,0),
    IDP_time_arrive=  ifelse(F.f2.idp_time_arrive %in% c("1_month","3_month"),1,0),
    IDP_majority=  ifelse( F.f2.idp_perc %in% c("half","more_half"),1,0),
    food_inadequate= ifelse(G.food_now == "no", 1,0),
    less_one_meal = ifelse(G.meals_number %in% c("one", "Less_than_1"),1,0),
    hunger_severe_worse = ifelse(S.shock_hunger %in% c("hunger_severe", "hunger_worst"),1,0),
    wildfood_sick_alltime = ifelse(G.food_wild_emergency=="yes"|G.food_wild_proportion=="all",1,0),
    skipping_days = ifelse(G.food_coping_comsumption.skip_days == "yes",1,0),
    flooded_shelter = ifelse(J.shelter_flooding == "yes",1,0),
    fsl_composite = (food_inadequate +less_one_meal+hunger_severe_worse+wildfood_sick_alltime+skipping_days)/5
  )

#extract new columns added (should be only composite). You can add new composites above and this will still work
vars_to_avg<-names(data_hex_pt_w_composite)[!names(data_hex_pt_w_composite)%in%names(data_hex_pt)]
analyzed_by_grid<-data_hex_pt_w_composite %>%
  group_by(State_id)%>%
  summarise_at(vars(vars_to_avg),mean, na.rm=T)

num_pts_per_grid<-data_hex_pt_w_composite %>% group_by(State_id) %>%
  summarise(number_pts=n()) %>% st_drop_geometry()

analyzed_by_grid<- analyzed_by_grid %>% left_join(num_pts_per_grid)

analyzed_by_grid_filt<-analyzed_by_grid %>%
  ungroup() %>%
  mutate_at(.vars = vars_to_avg,
            .funs = function(x){ifelse(analyzed_by_grid$State_id %in% valid_grids,x,NA)})


# write.csv(analyzed_by_grid_filt,"outputs/2020_02/aggregated_data/2020_04_23_reach_ssd_HEX_aggregated_AoK_FEB2020.csv")
analyzed_by_grid_filt
hex_grid %>% left_join(analyzed_by_grid_filt)




















aok_aggregated_4 %>% group_by(name_county_low) %>%
  mutate(num=n()) %>%
  filter(num>1)

aok_aggregated_3 %>%
  group_by(name_county_low) %>%
  mutate(num_rep=n()) %>%
  filter(num_rep>1)

########################################################3
aok_clean_aggregated_settlement %>% nrow()
aok_aggregated_4<-aok_aggregated_3 %>%
  mutate(
    NAME= D.info_settlement %>%  gsub("'","",.) %>% gsub(" ","-", .) %>% tolower(),
    NAMEJOIN= NAME  %>% gsub("-","_",.),
    name_merge=ifelse(NAMEJOIN =="pen_manyang","penmanyang",NAMEJOIN),
    name_merge=ifelse(name_merge=="totakuei", "tot_akuei", name_merge)
  ) %>%
  left_join(master_new %>%
              mutate(NAMEJOIN=NAMEJOIN  %>% gsub("-","_",.) %>% tolower()) %>%
              select(NAMEJOIN,COUNTYJOIN,X,Y) ,
            by=c("name_merge"= "NAMEJOIN",
                 "D.info_county"="COUNTYJOIN"))

aok_with_payams<-st_as_sf(aok_aggregated_4,coords = c("X","Y"),crs=4326) %>%
  st_join(payams %>% select(adm3_name)) %>%st_drop_geometry()
# aok_with_payams %>% View()




























master_new %>% filter(str_detect(NAMEJOIN ,"tot")) %>% arrange(NAME) %>% select(NAME)
aok_clean_aggregated_settlement %>% nrow()

aok_clean_aggregated_settlement %>%
  filter(name_county_low %in% master_new$name_county_low) %>% nrow()

master_new %>%
  filter(name_county_low %in% aok_clean_aggregated_settlement$name_county_low) %>% nrow()

aok_clean_aggregated_settlement %>% nrow()

?left_join
aok_clean_aggregated_settlement %>%
  left_join(master_new, by=c("name_county_low"="name_county_low"),)

aok_clean_aggregated2<-aok_clean_aggregated_settlement %>%
  mutate(
    NAME= D.info_settlement %>%  gsub("'","",.) %>% gsub(" ","-", .) %>% tolower(),
    NAMEJOIN= NAME  %>% gsub("-","_",.)
    ) %>%
  left_join(master_new %>%
              mutate(NAMEJOIN=NAMEJOIN  %>% gsub("-","_",.) %>% tolower()) %>%
                       select(NAMEJOIN,COUNTYJOIN,X,Y) ,
             by=c("NAMEJOIN"= "NAMEJOIN",
                  "D.info_county"="COUNTYJOIN"))




#join by name_county_low-- go with the one more KI
aok_clean_aggregated2<-aok_clean_aggregated_settlement %>%
  mutate(
    NAME= D.info_settlement %>%  gsub("'","",.) %>% gsub(" ","-", .) %>% tolower(),
    NAMEJOIN= NAME  %>% gsub("-","_",.)
    ) %>%
  left_join(master_new %>%
              mutate(NAMEJOIN=NAMEJOIN  %>% gsub("-","_",.) %>% tolower()) %>%
                       select(NAMEJOIN,COUNTYJOIN,NAME,name_county_low,X,Y) ,
             by=c("name_county_low"= "name_county_low"
                  ))

aok_clean_aggregated2 %>% filter(is.na(X))



mutate(D.ki_coverage2=sum(D.ki_coverage),
         num=n()) %>%
  filter(num>1) %>% select(NAMEJOIN,X,Y)
aok_clean_aggregated2<-aok_clean_aggregated_settlement %>%
  left_join(master_new ,
             by=c("name_county_low"= "name_county_low"))

aok_clean_aggregated2 %>% filter(is.na(Y)) %>% select(D.settlecounty, D.info_settlement,D.info_county, NAMEJOIN)

master_new %>% filter(str_detect(NAMEJOIN,"Tota"))

aok_clean_aggregated2<-aok_clean_aggregated_settlement %>%
  left_join(master_new %>% select(NAMEJOIN,COUNTYJOIN,NAMECOUNTY ,X,Y),
             by=c("D.settlecounty"= "NAMECOUNTY"))

aok_clean_aggregated2 %>%
aok_clean_aggregated_settlement$D.settlecounty
aok_clean_aggregated2 %>%
  filter()

aok_clean_aggregated2 %>%
  group_by(D.settlecounty) %>%
  mutate(asdf=n()) %>%
  filter(asdf>1) %>% select(X,Y)
aok_clean_aggregated_settlement %>% nrow()
aok_clean_aggregated2b %>% nrow()
aok_clean_aggregated2b<-aok_clean_aggregated2 %>%
  distinct(D.info_settlement,D.info_county, .keep_all = T)



aok_with_payams %>% nrow()


aok_clean_aggregated_settlement %>% nrow()

aok_clean_aggregated2 %>% nrow()
aok_clean_aggregated2b<-aok_clean_aggregated2 %>%
  distinct(X,Y, .keep_all = T)
aok_clean_aggregated2b %>% nrow()
aok_clean_aggregated2 %>%
  group_by(D.info_settlement, D.info_county) %>%
  mutate(num=n()) %>%
  filter(num>1) %>% arrange(name_county_low) %>% select(X,Y)
master_new_2 %>% filter(is.na(X))
new_sett_sf
aok_clean_aggregated2 %>% filter(is.na(X))




butteR::get_na_response_rates(aok_clean_aggregated2) %>% arrange(desc(perc_non_response)) #%>% write.csv('na_response_rate_butter_Feb1.csv')


kobo_tool<-"inputs2/2020_02/kobo_tool/2020_02_SSD_AoK_tool_38b.xlsx"
kt<-readxl::read_xlsx(path = kobo_tool, sheet = "survey")
# kt<- butteR::add_groups_to_xlsform_names(kt)
kc<-readxl::read_xlsx(path = kobo_tool, sheet = "choices")

aok_clean_aggregated3<-aok_clean_aggregated2
colnames(aok_clean_aggregated3)<- butteR::remove_kobo_grouper(colnames(aok_clean_aggregated2),max_prefix_length = 3) %>% butteR::remove_kobo_grouper(max_prefix_length = 3)

kt2<-kt %>%
  filter(name%in% colnames(aok_clean_aggregated3))

settlement_level_kobold<-kobold::kobold(survey = kt2,choices = kc,data = aok_clean_aggregated3)

settlement_level_kobold_SL<-kobold:::relevant_updater(settlement_level_kobold)

# settlement_level_kobold_SL$data %>% write.csv("fsdfakdlfaasdfkdalkfa_22222.csv")
aggregated_kobold$survey%>%
  filter(str_detect(relevant,"type_visit")) %>%
  select(relevant)





long_term_aok_latest_file<- "inputs2/2020_02/REACH_SSD_AoK_LongTermSettlementData.csv"
previous_long_term<-read.csv(long_term_aok_latest_file,stringsAsFactors = FALSE,
         row.names = NULL, na.strings = c(""," ",NA, "NA","n/a"),
         strip.white = TRUE)

colnames(previous_long_term)[!colnames(previous_long_term)%in% colnames(aok_clean_aggregated_settlement)]
colnames(aok_clean_aggregated_settlement)[!colnames(aok_clean_aggregated_settlement)%in% colnames(previous_long_term)]
nrow(previous_long_term)
previous_long_term %>% filter(!is.na(L.ag_inputs)) %>% nrow()
previous_long_term$L.ag_inputs
colnames(previous_long_term)<- colnames(previous_long_term) %>%
  str_replace_all(c("L.current_activities.Crops_for_sustenance"="L.current_activities.crops_for_sustenance",
                    "L.current_activities.Livestock"= "L.current_activities.livestock"))

long_term_aggregated_new<-bind_rows(previous_long_term, aok_clean_aggregated_settlement)
long_term_aggregated_new %>% filter(!is.na(L.ag_inputs))
# write.csv(aok_clean_aggregated_settlement,aggregated_file_name, na="SL")
# write.csv( long_term_aggregated_new,aggregated_long_term_file_name,na = "SL")

## Hexagonal Aggregation
#READ IN HEX GRID
hex_grid <- st_read(dsn = "inputs/GIS",layer ="Grids_info") %>%
  mutate( id_grid = as.numeric(rownames(.)))

master_sett_new<-master_new %>%
  mutate(id_sett = as.numeric(rownames(.))) %>%
  st_as_sf(coords=c("X","Y"), crs=4326) %>%
  st_transform(crs=st_crs(hex_grid)) %>%
  select(NAME:COUNTYJOIN)

aok_clean4<-aok_clean4 %>%
  mutate(date=month_of_assessment %>% ymd(),
         month=month(date),
         year=year(date),
         #if we use D.info_settlement_final the others wont match until clean
         D.settlecounty=paste0(D.info_settlement,D.info_county)) %>%
  # therefore use D.info_settlement and filter others for tutorial
  filter(D.info_settlement!="other")

sett_w_grid <- st_join(master_sett_new, hex_grid)
assessed_w_grid <-inner_join(sett_w_grid, aok_clean4, by = c("NAMECOUNTY"="D.settlecounty") )


#Aggregate the data to the hexagon grid-level through the following steps:
#  1. Calculate # KIs per settlement (D.ki_coverage)
#  2. Calculate the # of settlements/grid,  and # KIs/grid
#  3. Filter out grids with less than 2 KIs or Settlements (would be good to have citation for this rule).


grid_summary<-assessed_w_grid %>%
  group_by(NAMECOUNTY,State_id) %>%
  summarise(D.ki_coverage=n()) %>%
  group_by(State_id) %>%
  summarise(settlement_num=n() ,ki_num=sum(D.ki_coverage) )

#Filter Grids with less than 2 KIs
grid_summary_thresholded <- grid_summary %>% filter(ki_num > 1, settlement_num > 1)


#Next we will create composite indicators to analyze at the grid level. This may need to be edited to add or #remove composite indicators later.


#create new composites
assessed_w_grid_w_composite<-assessed_w_grid %>%
  mutate(
    idp_sites= ifelse(J.j2.idp_location=="informal_sites",1,0),
    IDP_present= ifelse(F.idp_now=="yes",1,0),
    IDP_time_arrive=  ifelse(F.f2.idp_time_arrive %in% c("1_month","3_month"),1,0),
    IDP_majority=  ifelse( F.f2.idp_perc %in% c("half","more_half"),1,0),
    food_inadequate= ifelse(G.food_now == "no", 1,0),
    less_one_meal = ifelse(G.meals_number %in% c("one", "Less_than_1"),1,0),
    hunger_severe_worse = ifelse(S.shock_hunger %in% c("hunger_severe", "hunger_worst"),1,0),
    wildfood_sick_alltime = ifelse(G.food_wild_emergency=="yes"|G.food_wild_proportion=="all",1,0),
    skipping_days = ifelse(G.food_coping_comsumption.skip_days == "yes",1,0),
    flooded_shelter = ifelse(J.shelter_flooding == "yes",1,0),
    fsl_composite = (food_inadequate +less_one_meal+hunger_severe_worse+wildfood_sick_alltime+skipping_days)/5
  )

#extract new columns added (should be only composite). You can add new composites above and this will still work
vars_to_avg<-names(assessed_w_grid_w_composite)[!names(assessed_w_grid_w_composite)%in%names(assessed_w_grid)]

analyzed_by_grid<-assessed_w_grid_w_composite %>%
  group_by(id_grid, State_id,month,year,date,D.info_state, D.info_county)%>%
  summarise_at(vars(vars_to_avg),mean, na.rm=T)




#Once analyzed you can write the aggreagted data to a csv or left_join it to the original hex data and write #it out as a polygon straight for mapping.


#Filter Grids with less than 2 KIs

analyzed_by_grid_thresholded<-analyzed_by_grid %>%
  filter(State_id %in% grid_summary_thresholded$State_id)
# dir.create("outputs/2020_02")
hex_aggregated_file_name<- paste0(
                                        iso_date,
                                        "_reach_ssd_HEX_aggregated_AoK_",month_label,"2020.csv")
# write.csv(analyzed_by_grid_thresholded,paste0("outputs/2020_02/",hex_aggregated_file_name))
# write.csv(
# analyzed_by_grid_thresholded,
# file = paste0(month_of_assessment %>% str_replace_all("-","_"),"_AoK_hex_aggregations.csv"),
# na = "NA",
# row.names = FALSE)


hex_grid_polygon_with_aggregated_data<-hex_grid %>% left_join(analyzed_by_grid_thresholded %>% st_drop_geometry())
hex_grid_polygon_with_aggregated_data %>%


# or write it out to a polgon file for mapping
#using st_write function









