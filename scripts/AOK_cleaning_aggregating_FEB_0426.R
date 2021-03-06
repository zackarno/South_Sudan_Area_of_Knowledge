
library(tidyverse)
library(butteR)
library(koboloadeR)
library(lubridate)
library(sf)

iso_date<- Sys.Date() %>%  str_replace_all("-","_")
month_input_data<-"2020-02-01"

source("scripts/functions/aok_aggregation_functions.R")
source("scripts/functions/aok_cleaning_functions.R")
# source("scripts/functions/aok_aggregate_settlement3.R")
source("scripts/functions/aok_set_paths.R")



#SETTINGS TO TOGGLE DEPENDING ON STAGE OF DATA
fuzzy_settlement_matching_done<-c("yes", "no")[1]
output_data_issues<-c("yes","no")[2]
output_cleaning_logs<-c("yes","no")[2]
output_new_settlement_data<-c("yes","no")[2]
output_aggregated_datasets<-c("yes","no")[1]
cleaning_log_format<-c("multiple_csvs","multiple_xlsx")[1]
#theses will get filled and written if selected
cleaning_log_list<-list()
data_issues<-list()


#READ COUNTY LEVEL (ADM2) AND PAYAM LEVEL (ADM3) BOUNDARY FILES AND CONVERT TO WGS84 WHERE NECESSARYS
adm2<- st_read(gdb,"ssd_admbnda_adm2_imwg_nbs_20180401", stringsAsFactors = F ) %>% st_transform(adm2,crs=4326)
payams<-st_read(gdb, "ssd_bnd_adm3_wfpge_un", stringsAsFactors = F )

master_settlement<-read.csv(master_settlement_list_from_previous_round_input_path,
                  strip.white = T, stringsAsFactors = T,na.strings = c(" ",""))

#THIS IS USEFUL LATER WHEN WE WILL BE DOING FUZZY MATCHING DISTANCE/NAME MATCHING TO KNOW WHICH COLUMNS COME FROM MASTER
colnames(master_settlement)<-paste0("mast.",colnames(master_settlement))
master_settlement_sf<- st_as_sf(master_settlement,coords=c("mast.X","mast.Y"), crs=4326)

itemset_previous<-read.csv(itemset_previous_month_input_file,strip.white = T, stringsAsFactors = T,na.strings = c(" ",""))



# LOAD RAW DATA -----------------------------------------------------------

aok_raw_list <-butteR::read_all_csvs_in_folder(input_csv_folder = raw_data_folder)

purrr::map(aok_raw_list,nrow)

aok_raw<- dplyr::bind_rows(aok_raw_list)
aok_raw %>% nrow()


#MAKE SURE ALL UUIDS ARE UNIQUE BECAUSE SOMETIMES DATA COMES FROM MULTIPLE SOURCES
aok_raw <- distinct(aok_raw,X_uuid, .keep_all= TRUE)

#THIS BUTTER FUNCTION WORKS EVEN THOUGH THEY DOWNLOAD THE DATA WITH GROUPS
sm_cols_df<-butteR::extract_sm_option_columns(df = aok_raw,name_vector=colnames(aok_raw))

convert_logical_to_yn<-function(x){
  x %>% as.character() %>% str_replace_all(c("1"="yes","0"="no"))
}

#SSD DOWNLOADS SELELCT MULTIPLE AS BINARY (0,1) INTEGERS- CONVERT THESE TO YES,NO
aok_raw<-aok_raw %>%
  mutate_at(.vars= sm_cols_df$sm_options,.funs = convert_logical_to_yn)


# JUST AN EARLY CHECK TO MAKE SURE ALL SETTLEMENTS LISTED IN MAIN SETTLEMENT COLUMN EXIST IN THE MASTER DATA SET- THEY BETTER
aok_raw<-aok_raw %>%
  mutate(name_county_low= paste0(D.info_settlement,D.info_county) %>% butteR::tolower_rm_special())

master_settlement_sf<-master_settlement_sf %>%
  mutate(name_county_low= mast.NAMECOUNTY %>% butteR::tolower_rm_special())

check_all_settlements_against_master<-aok_raw%>%
  mutate(name_county_low= paste0(D.info_settlement, D.info_county) %>% butteR::tolower_rm_special()) %>%
  filter(D.info_settlement!="other") %>%
  filter(!name_county_low%in%master_settlement_sf$name_county_low) %>% as_tibble() %>%
  select(D.info_county,D.info_settlement, name_county_low)
if(nrow(check_all_settlements_against_master)==0){
  print("good - all settlements are contained in the master data set")
}else{
  print("settlements in data set that are not in master")
}


# STEP 1 COMPILE CLEANING LOGS --------------------------------------------

if(cleaning_log_format=="multiple_csvs"){
  cleaning_logs<-butteR::read_all_csvs_in_folder(input_csv_folder = field_cleaning_logs_folder_path)
  cleaning_log<-bind_rows(cleaning_logs)
}
if(cleaning_log_format=="multiple_xlsx"){
  xlsx_files<-list.files(field_cleaning_logs_folder_path, full.names = T)
  xlsx_file_names<-list.files(field_cleaning_logs_folder_path, full.names = F)
  cleaning_logs<-list()
  for(i in 1: length(xlsx_files)){
    file_name_temp<- xlsx_file_names[i]
    path_temp<-xlsx_files[i]
    print(file_name_temp)
    df<-readxl::read_xlsx(path = path_temp, sheet = "cleaning_log")
    cleaning_logs[[file_name_temp]]<-df
  }
  cleaning_log<-bind_rows(cleaning_logs)}
#QUICK CHECK -- SOMETIMES AOS PROVIDE DIFFERENT NAMES FOR UID
num_uuid_in_cl<-cleaning_log %>% select(contains("uid")) %>% colnames() %>% length()
if(num_uuid_in_cl>1){
  print(cleaning_log %>% select(contains("uid")) %>% colnames() )
}else(print("GOOD- only one uuid column found"))

if(month_input_data=="2020-02-01"){
  cleaning_log<- cleaning_log %>%
  mutate(uuid=ifelse(!is.na(uuid),uuid,ï..uuid)) %>% select(-ï..uuid)}
#IF THERE IS MORE THAN ONE YOU HAVE TO ADJUST

#ADD BASE TO CLEANING LOG
cleaning_log<- cleaning_log %>% left_join(aok_raw %>%
                                            select(X_uuid,A.base ), by=c("uuid"="X_uuid"))

cleaning_log_actionable<- cleaning_log %>% filter(change_type!="no_action")

# MULTIPLE CLEANING LOGS WILL BE CREATED FOR FINAL DOCUMENTATION, FOR THE TIME BEING LETS STORE IN A LIST

cleaning_log_list[["main"]]<-cleaning_log_actionable %>% mutate(source="field")

# CHECK CLEANING LOG
aok_cleaning_checks1<-butteR::check_cleaning_log(df = aok_raw, df_uuid = "X_uuid",
                                                cl = cleaning_log_actionable,
                                                cl_change_type_col = "change_type",
                                                cl_change_col = "indicator",
                                                cl_uuid = "uuid",
                                                cl_new_val = "new_value")

#VIEW PROBLEMS IN CLEANING LOG

data_issues[["field_CL_issues"]]<-aok_cleaning_checks1

aok_cleaning_checks1 %>% class()
#WE WILL HAVE TO DELETE THESE BECAUSE THERE WAS NO REVISION FROM THE FIELD (ITS OK)
if(is.character(aok_cleaning_checks1)==F){
cleaning_log_actionable<- cleaning_log_actionable %>%
  filter(!uuid%in%aok_cleaning_checks1$uuid)}

cleaning_log_list[["field_cl"]]<-cleaning_log_actionable %>% mutate(source="field")

#RE-RUN CHECKS WITH FILTERED CLEANING LOG... SHOULD BE  NO PROBLEM
aok_cleaning_checks2<-butteR::check_cleaning_log(df = aok_raw, df_uuid = "X_uuid",
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

if(aok_clean %>%
  filter(D.info_settlement!="other") %>%
  filter(!name_county_low%in% master_settlement_sf$name_county_low) %>% nrow()>0){
  print("BAD - cleaning log has added a new settlement that doesnt belong in new settlement list")
}else{
  print("GOOD- no settlement errors introduced in cleaning log")
}


# CREATE AOK_CLEAN
# STEP 2 NEW SETTLEMENTS --------------------------------------------------
# PERHAPS THE MOST DIFFICULT STEP
# OUTPUTS: NEW ITEMSET, NEW MASTER SETTLEMENT FILE, CLEANING LOG

#READ IN NEW SETTLEMENTS FROM THE FIELDS
new_settlements<- butteR::read_all_csvs_in_folder(new_settlement_folder)
 new_settlements$new_settlements_test.csv %>% col
new_sett<- bind_rows(new_settlements)
# new_sett %>% View()

# AOS INSTRUCTED TO TAKE THE NEW SETTLEMENT LIST DIRECTLY FROM THE DATA, BUT IN CASE THIS INSTRUCTION IS NOT FOLLOWED
# THIS JUST CORRECTS ANY OF THE NEW SETTLEMENTS TO THE DATA BY THE UUID (TOOK HOURS TO REALIZE THIS FUNCTION COULD AVOID HEADACHE LATER ON)

if(month_input_data=="2020-02-01"){
  new_sett<- new_sett %>%
    filter(action=="Map") %>%
    rename(uuid="ï..uuid",
           latitude="lat",
           longitude="long")

  }


new_sett<-rectify_new_settlements_with_data( new_settlements = new_sett,aok =  aok_clean)

if(month_input_data!="2020-02-01"){
new_sett<-new_sett %>%
  mutate(
    D.info_settlement_other=ifelse(is.na(D.info_settlement_other),New.settlements,D.info_settlement_other)
  )}


# IF THE CLEANING LOGS DO CONTAIN ENTRIES WHERE THY HAVE CHANGED SETTLEMENTS IN DATA, BUT NOT IN NEW SETTLEMENTS, THIS WILL
# ESSENTIALLY MAKE A LOOK UP TABLE, IF NOT ITS FINE

sett_aok_id_key<-cleaning_log_actionable %>%
  filter(str_detect(indicator, "settlement")) %>%
  select(uuid, indicator,Current_value, new_value) %>%
  filter(Current_value=="other")
sett_aok_id_key<- aok_raw %>%
  inner_join(sett_aok_id_key, by= c("X_uuid"="uuid")) %>%
  select(old_value=D.info_settlement_other, new_value)

new_sett_with_key<- new_sett %>%
  left_join(sett_aok_id_key,
            by= c("D.info_settlement_other"="old_value"))

# IF THEY HAVE CHANGED THE COLUMN D.INFO_SETTLEMENT_OTHER COLUMN WITH THE CLEANING LOG, THIS WILL SWITCH THE VAR OF INTEREST
# TO REFLECT THAT CHANGE IN THE NEW SETTLEMENT SHEET
new_sett2<- new_sett_with_key %>%
  mutate(name_new_settlement= ifelse(!is.na(new_value), new_value, D.info_settlement_other)) %>%
  select(-D.info_settlement_other)



#CANT DO ANYTHING WITH NEW SETTLEMENTS THAT DONT HAVE COORDINATES
new_sett3<-new_sett2 %>%
  filter(!is.na(longitude), !is.na(latitude))


new_sett_no_coords<-new_sett2 %>% filter(is.na(longitude)|is.na(latitude))
if(nrow(new_sett_no_coords)>0){
  data_issues[["new_settlement_sheet_entries_no_COORDS"]]<-new_sett_no_coords
}

new_sett_sf<-st_as_sf(new_sett3,coords=c("longitude","latitude"), crs=4326)

#this is a good visual check to see if points fall out of SSD
ggplot()+geom_sf(data=adm2)+
  geom_sf_label(data= new_sett_sf,aes(label =name_new_settlement))+
  geom_sf(data=new_sett_sf)


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

remove_from_new_sett<-aok_clean %>%
  filter(X_uuid %in% new_sett_sf$uuid  & is.na(D.info_settlement_other))%>%
  select(X_uuid,D.info_settlement) %>% pull(X_uuid)

# THIS CONTINUES TO BE A MASSIVE PROBLEM FOR THE FIELD -- ENUMERATORS PUT A NEW SETTLEMENT, BUT IT
# IS NOT CAPTURED IN THE NEW SETTLEMENT TAB. NOT REALLY ACCEPTABLE, BUT THERE IS NOTHING WE CAN DO AT THIS POINT
# OTHER THAN OUTPUT THE ERRORS AND HOPE THEY CAN FIX THEM (NO LUCK SO FAR)

aok_other_not_in_settlement_data<-aok_clean %>%
  filter(!is.na(D.info_settlement_other)) %>%
  filter(!X_uuid %in% new_sett_sf$uuid) %>%
  select(X_uuid,A.base,D.info_settlement, D.info_settlement_other) %>%
  arrange(A.base) # %>%

if(nrow(aok_other_not_in_settlement_data)>0){
data_issues[["AoK_other_settlements_no_coordinates_in_new_settlement"]]<-aok_other_not_in_settlement_data}




# EVEN AFTER CLEANING FROM THE FIELD THERE ARE SETTLEMENTS GIVEN AS NEW SETTLEMENTS, BUT THEY MATCH EXACTLY NEW SETTLEMENTS
# IN THE CHOICES AND MASTER SETTLEMENT LIST. THESE SHOULD REALLY BE CAUGHT, BUT THIS WILL ALSO CATCH THEM.

# SOMETIMES THIS IS DUE TO THE FACT THAT THE ENUMERATOR/DATA CLEANER PUT THE WRONG COUNTY. SINCE THIS BEEN SPATIALLY JOINED WITH THE COUNTY
# SHAPEFILE WE CAN CHECK THIS AS WELL

#THE `exact_matches1`  OUTPUT WILL SERVE AS AN INPUT TO MAKE CLEANING LOG TO FIX THE DATA
exact_matches1<-new_sett_sf %>%
  mutate(matched_where= case_when(new.enum_sett_county %in%
                                    master_settlement_sf$mast.settlement_county_sanitized~"enum", #CHECK WITH ENUMS INPUT
                                  new.adm2_sett_county %in%
                                    master_settlement_sf$mast.settlement_county_sanitized~"shapefile_only" ),
         #IF IT MATCHES ON THE SHAPE FILE WE SHOULD USE THAT RATHER THAN THE DATA ENTRY
         county_use= ifelse(matched_where=="shapefile_only",adm2,D.info_county)) %>%
  filter(!is.na(matched_where)) #ONLY RETURN EXACT MATCHES

# WRITE EXACT MATCHES TO CLEANING LOG TO THEN IMPLEMENT ON DATA.
# THIS WRITES OUT A CLEANING LOG WHICH CORRECTS THE SETTLEMENT ENTRY AS WELL AS COUNTY WHERE NECESSARY
aok_exact_matches_cl<-exact_matches_to_cl(exact_match_data = exact_matches1,
                                          user = "Zack",
                                          uuid_col = "uuid",
                                          settlement_col = "name_new_settlement")

# IF THERE IS A FAULTY UUID IN THE NEW SETTLEMENT DATA THIS WILL GET RID OF IT
aok_exact_matches_cl_filt<-aok_exact_matches_cl %>%
  filter(uuid %in% aok_clean$X_uuid)

#ADD THIS NEW CLEANIGN LOG TO THE LIST
cleaning_log_list[["auto_gen_exact_matches"]]<- aok_exact_matches_cl_filt %>% mutate(source="auto-generated")

# IMPLEMENT THE NEW LOG
aok_clean2<-butteR::implement_cleaning_log(df = aok_clean,df_uuid = "X_uuid",
                                           cl = aok_exact_matches_cl_filt,
                                           cl_change_type_col = "change_type",
                                           cl_change_col = "indicator",
                                           cl_uuid = "uuid",
                                           cl_new_val = "new_value")



# FUZZY NEW SETTLEMENT MATCHING -------------------------------------------

#REMOVE THE SETTLEMENTS THAT EXACT MATCHES FROM THE LIST
new_sett_sf_unmatched<- new_sett_sf %>% filter(!uuid %in% exact_matches1$uuid)


#REMOVE MATCHED SETTLEMENTS FROM MASTER

master_settlement_sf_not_matched<-master_settlement_sf %>%
  filter(!mast.settlement_county_sanitized %in% c(new_sett_sf_unmatched$new.enum_sett_county, new_sett_sf$new.adm2_sett_county))

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


if(fuzzy_settlement_matching_done=="no"){
  new_settlement_evaluation<-evaluate_unmatched_settlements(user= "zack",new_settlement_table = settlements_best_guess, uuid_col="uuid")
  if(write_auto_gen_cleaning_logs=="yes"){
    write.csv(new_settlement_evaluation$checked_setlements,fuzzy_match_eval_table_output_path)
    write.csv(new_settlement_evaluation$cleaning_log,fuzzy_match_CL_output_path)

  }

}

if(fuzzy_settlement_matching_done=="yes"){
  new_settlement_evaluation<-list()
  new_settlement_evaluation$checked_setlements<-read_csv(fuzzy_match_eval_table_output_path)
  new_settlement_evaluation$cleaning_log<-read_csv(fuzzy_match_CL_output_path)

}






new_settlement_evaluation$cleaning_log %>% nrow()

auto_gen_settlement_evaluated_CL_check<-butteR::check_cleaning_log(df = aok_clean2,df_uuid = "X_uuid",
                                           cl =new_settlement_evaluation$cleaning_log,
                                           cl_change_type_col = "change_type",
                                           cl_change_col = "suggested_indicator",
                                           cl_uuid = "uuid",
                                           cl_new_val = "suggested_new_value")



if(is.character(auto_gen_settlement_evaluated_CL_check )==F){
  print("Problem with Log:")
  print(auto_gen_settlement_evaluated_CL_check)
}

# HERE IT IS BECAUSE THESE ROWS OF DATA WERE DROPPED IN AN EARLIER CLEANING LOG
# CLEANING PROCESS-- SO JUST REMOVE THEM FROM THE LOG
new_settlement_evaluation$cleaning_log<- new_settlement_evaluation$cleaning_log %>%
  filter(uuid %in%  aok_clean2$X_uuid)
auto_gen_settlement_evaluated_CL_check2<-butteR::check_cleaning_log(df = aok_clean2,df_uuid = "X_uuid",
                                                                   cl =new_settlement_evaluation$cleaning_log,
                                                                   cl_change_type_col = "change_type",
                                                                   cl_change_col = "suggested_indicator",
                                                                   cl_uuid = "uuid",
                                                                   cl_new_val = "suggested_new_value")
if(is.character(auto_gen_settlement_evaluated_CL_check2)==F){
  print("Problem with Log:")
  print(auto_gen_settlement_evaluated_CL_check2)
}


aok_clean3<-butteR::implement_cleaning_log(df = aok_clean2,df_uuid = "X_uuid",
                                           cl =new_settlement_evaluation$cleaning_log,
                                           cl_change_type_col = "change_type",
                                           cl_change_col = "suggested_indicator",
                                           cl_uuid = "uuid",
                                           cl_new_val = "suggested_new_value")





new_settlement_evaluation$cleaning_log_cols_modified<-new_settlement_evaluation$cleaning_log %>% select(uuid,action,spotted,change_type,Sectors,suggested_indicator:suggested_new_value)
colnames(new_settlement_evaluation$cleaning_log_cols_modified)<- colnames(new_settlement_evaluation$cleaning_log_cols_modified) %>% str_replace_all("suggested_","")
cleaning_log_list[["auto_gen_settlements_evaluated"]]<- new_settlement_evaluation$cleaning_log_cols_modified %>% mutate(source="auto-generated")



# UPDATE ITEMSET FOR KOBO TOOL WITH NEW SETTLMENTS ------------------------

new_sets_to_add_itemset<-new_settlement_evaluation$checked_setlements %>%
  filter(action==2) %>%
  mutate(
    list_name="settlements",
    label= new.D.info_settlement_other %>% gsub("_","-", .)
  ) %>%
  select(list_name,name=new.D.info_settlement_other, label,admin_2=new.county_adm2)

# read in previous itemset

itemset_not_other<-itemset_previous %>% filter(name!="other")
itemset_other<- itemset_previous %>% filter(name=="other")


itemset_binded<-bind_rows(list(new_sets_to_add_itemset,itemset_not_other)) %>% arrange(admin_2)
itemset_full_binded<- bind_rows(list(itemset_binded,itemset_other))


if(output_new_settlement_data=="yes"){
write.csv(itemset_full_binded,itemset_output_file_name)}


# NEXT WE ADD THE NEW SETTLEMENTS TO THE SHAPEFILE ------------------------

# just read the data in fresh to avoid any mistake of objects already in memory
master_settlement<-read.csv(master_settlement_list_from_previous_round_input_path, stringsAsFactors = FALSE)

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


#JUST GRAB RELEVANT
master_settlement_adj<- master_settlement_adj %>%
  select(NAME:IMG_VERIFD,INDEX:name_county_low, X= X_avg, Y=Y_avg)

# new_sett_sf

new_setts_add_to_master<-new_settlement_evaluation$checked_setlements %>%
  filter(action==2) %>%
  mutate(
    uuid= uuid,
    NAME= new.D.info_settlement_other %>%  gsub("'","",.) %>% gsub(" ","-", .),
    NAMEJOIN= NAME  %>% gsub("-","_",.),
    NAMECOUNTY=paste0(NAMEJOIN,new.county_adm2),
    COUNTYJOIN= new.county_adm2 ,
    DATE= month_input_data %>% ymd(),
    DATA_SOURC="AOK",
    IMG_VERIFD= 0
  ) %>% #get coordinates from field data back in
  # select(uuid,NAME,NAMEJOIN,NAMECOUNTY,COUNTYJOIN,DATE,DATA_SOURC,IMG_VERIFD,X,Y ) %>%
  left_join(new_sett_sf %>%st_drop_geometry_keep_coords() ,
            by=c("uuid"="uuid")) %>%
  select(-uuid,NAME,NAMEJOIN,NAMECOUNTY,COUNTYJOIN,DATE,DATA_SOURC,IMG_VERIFD,X,Y ) %>%
  distinct()

new_setts_add_to_master<-new_setts_add_to_master %>% select(NAME,NAMEJOIN,NAMECOUNTY,COUNTYJOIN,DATE,DATA_SOURC,IMG_VERIFD,X,Y )



new_setts_add_to_master$DATE<-new_setts_add_to_master$DATE %>% as.character()
master_new<-bind_rows(new_setts_add_to_master,master_settlement_adj )
master_new %>% nrow()
master_settlement_adj %>% nrow()+ nrow(new_setts_add_to_master)



if(output_new_settlement_data=="yes"){
  write.csv(master_new,new_master_settlement_output_path)
  }





# UNFORTUNATELY IF THERE IS NO FEEDBACK FROM FIELD -- NEW SETTLEMENTS WITH NO LOCATIONS WILL NEED TO BE DROPPED

new_setts_with_no_mapping_uuid<-aok_clean3 %>%
  filter(!is.na(D.info_settlement_other)) %>%
  filter(!X_uuid %in% new_sett_sf$uuid) %>%
  select(X_uuid,A.base,D.info_settlement, D.info_settlement_other) %>%
  pull(X_uuid)


aok_clean4<-aok_clean3 %>%
  filter(!X_uuid %in% new_setts_with_no_mapping_uuid )

#LETS JUST TURN THIS FILTER INTO A CLEANING LOG FOR DOCUMENTATION PURPOSES
cleaning_log_settlements_dropped<-aok_clean3 %>%
  filter(!is.na(D.info_settlement_other)) %>%
  filter(!X_uuid %in% new_sett_sf$uuid) %>%
  mutate(change_type = "remove_survey",
         spotted="Zack",
         Sectors="Area_of_Knowledge",
         indicator= "D.info_settlement_other",
         current_value= D.info_settlement_other,
         new_value=NA,
         issue = "No GPS for new settlement provided in New Settlement list"
         ) %>%
  select(uuid=X_uuid, change_type:issue)
cleaning_log_list[["record_dropped_because_missed_in_CL_and_new_settlement_list"]]<-cleaning_log_settlements_dropped %>% mutate(source="auto-generated")

# THERE SHOULD BE NO MORE D.INFO_SETTLEMENT_OTHER
if(aok_clean4 %>% filter(!is.na(D.info_settlement_other)) %>%
  select(D.info_settlement, D.info_settlement_other) %>% nrow()>0){
  print("BAD- There are still some settlements listed as other")
} else {
  print("GOOD - no more settlents listed as other")
}

#maybe change the way assessment month is represented


#next start with the rmeove grouper stuff.


#good check!
aok_clean4<- aok_clean4 %>%
  mutate(name_county_low= paste0(D.info_settlement, D.info_county) %>%
           butteR::tolower_rm_special())# %>%

master_new<-master_new %>%
  mutate(name_county_low=  NAMECOUNTY %>% tolower_rm_special())

aok_clean4 %>%   filter(!name_county_low%in%master_new$name_county_low) %>% as_tibble() %>%
  select(D.info_county,D.info_settlement,D.info_settlement_other, name_county_low)

colnames(cleaning_log_list$field_cl)[2]<-"spotted"
colnames(cleaning_log_list$main)[2]<-"spotted"
cleaning_log_list<-purrr:::map(cleaning_log_list,rename_all, .funs = tolower)
cleaning_log_final<-bind_rows(cleaning_log_list )
cleaning_log_final %>% colnames()
cleaning_log_final<-cleaning_log_final %>% left_join(aok_raw %>% select(X_uuid,A.base), by=c("uuid"="X_uuid")) %>%
  select(uuid:issue,a.base,source)
output_cleaning_logs<-"yes"
if(output_cleaning_logs=="yes"){
  write.csv(cleaning_log_final,cleaning_log_output_file,na = "")
}

if(output_data_issues=="yes"){
  for(i in 1:length(data_issues)){
    name_di_temp<-names(data_issues)[i]
    di_temp<-data_issues[[i]]
    write.csv(di_temp,paste0(data_issues_output_prefix,name_di_temp,"_",month_label,"Data.csv"))
  }
}
data_issues$field_CL_issues
data_issues$new_settlement_sheet_entries_no_COORDS
data_issues$AoK_other_settlements_no_coordinates_in_new_settlement


# clean_data<-read.csv(clean_csv)
#output from butteR
# colnames(analysis_name,basic_analysis)
# names_ordered<-colnames(clean_data)




# AGGREGATE SETTLEMENTS USING WRAPPED SCRIPT ------------------------------

#THIS USED IN THE INPUT FILE SHOULD PROBABLY NOT BE THE LT BUT RATHER JUST PREVIOUS MONTH
prev_round<-read.csv(prev_round_LT_input_path, stringsAsFactors = FALSE, na.strings=c("", " ", NA, "NA"))


ks<-readxl::read_xlsx(path = kobo_tool_input_path, sheet = "survey")
kc<-readxl::read_xlsx(path = kobo_tool_input_path, sheet = "choices")


# debugonce(aggregate_aok_by_settlement)

aggregations_script<-c("v2","v3")[2]
if(aggregations_script=="v2"){
source("scripts/functions/aok_aggregate_settlement2.R")
aok_clean_aggregated_settlement<-aggregate_aok_by_settlement(clean_aok_data = aok_clean4,
                                                             current_month = month_input_data,
                                                             aok_previous = prev_round
)}
if(aggregations_script=="v3"){
source("scripts/functions/aok_aggregate_settlement3.R")
aok_clean_aggregated_settlement<-aggregate_aok_by_settlement(clean_aok_data = aok_clean4,
                                                              current_month = month_input_data,
                                                              kobo_survey_sheet = ks)}



# use kobold package to impute skip logic ---------------------------------
# first need to remove groups
colname_table<-data.frame(no_groups=butteR::remove_kobo_grouper(colnames(aok_clean_aggregated_settlement),max_prefix_length = 3) %>% butteR::remove_kobo_grouper(max_prefix_length = 3), with_groups=colnames(aok_clean_aggregated_settlement))
aok_clean_aggregated2<-aok_clean_aggregated_settlement
colnames(aok_clean_aggregated2)<- colname_table$no_groups

# have to get rid of rows in kobo survey sheet that are no longer in aggregated data first
ks2<-ks %>%
  filter(name%in% colnames(aok_clean_aggregated2))

settlement_level_kobold<-kobold::kobold(survey = ks2,choices = kc,data = aok_clean_aggregated2)
settlement_level_kobold_SL<-kobold:::relevant_updater(settlement_level_kobold)

aok_aggregated_3<-settlement_level_kobold_SL$data
colnames(aok_aggregated_3)<- colname_table$with_groups

#JOIN PAYAM DATA
aok_aggregated_3<- aok_aggregated_3 %>%
  mutate(
    name_county_low=paste0(D.info_settlement,D.info_county) %>% butteR::tolower_rm_special())

aok_aggregated_3 %>% nrow()
aok_aggregated_4<-aok_aggregated_3 %>% left_join(master_new %>% select(X,Y,name_county_low), by="name_county_low")
aok_aggregated_4 %>% nrow()
aok_aggregated_5<- aok_aggregated_4 %>% distinct() # one got duplicated - remove it (its ok its one that should be edited in itemset in future to just be one)
aok_aggregated_5 %>% nrow()
aok_aggregated_with_payams<-st_as_sf(aok_aggregated_5,coords = c("X","Y"),crs=4326) %>%
  st_join(payams %>% select(adm3_name))




# merge with long term data -----------------------------------------------

previous_long_term<-read.csv(prev_round_LT_input_path,stringsAsFactors = FALSE,
                             row.names = NULL, na.strings = c(""," ",NA, "NA","n/a"),
                             strip.white = TRUE)



colnames(previous_long_term)<- colnames(previous_long_term) %>%
  str_replace_all(c("L.current_activities.Crops_for_sustenance"="L.current_activities.crops_for_sustenance",
                    "L.current_activities.Livestock"= "L.current_activities.livestock"))
aok_aggregated_with_payams$month<- aok_aggregated_with_payams$month %>% as.character()
aok_aggregated_with_payams_df<-aok_aggregated_with_payams %>% st_drop_geometry()
long_term_aggregated_new<-bind_rows(previous_long_term, aok_aggregated_with_payams_df)


if(output_aggregated_datasets=="yes"){
write.csv(aok_aggregated_with_payams_df,settlement_aggregated_monthly_data_output_file, na="SL")
write.csv( long_term_aggregated_new,settlement_aggregated_LT_data_output_file)
}



# MAKE HEX GRIDS FOR CCCM FS ----------------------------------------------

mast_settlement<-read.csv(new_master_settlement_output_path, stringsAsFactors = FALSE, na.strings=c(""," ")) %>%
  mutate(name_county_low=NAMECOUNTY %>% tolower_rm_special())

#good quick check to make sure we have all settlementsin master list
aok_aggregated_with_payams %>%
  filter(!name_county_low %in% mast_settlement$name_county_low) %>%
  select(D.info_state,D.info_county, D.info_settlement)

aok_monthly<-read.csv(settlement_aggregated_monthly_data_output_file, stringsAsFactors = F, na.strings=c(""," ")) %>% select(-X) %>%
  mutate(name_county_low= paste0(D.info_settlement, D.info_county) %>%tolower_rm_special())
aok_with_coordinates <-aok_monthly %>% left_join(mast_settlement %>% select(X,Y,name_county_low), by="name_county_low")



# aok_with_coordinates %>% nrow()
aok_sf<-st_as_sf(aok_with_coordinates,coords = c("X","Y"),crs=4326)

hex_grid <- st_read(dsn = "inputs/gis_data",layer ="Grids_info", stringsAsFactors=F ) %>% st_transform(crs=4326) %>% select(-settlement)



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

data_hex_pt$F.f2.idp_perc %>% unique()
data_hex_pt$G.food_wild_proportion %>% table(useNA = "ifany")

kc %>% filter(list_name=="hc_leave_time") %>% select(name)
ks %>% filter(name=="idp_time_arrive") %>% select(type)

less_one_meal_var<- ifelse(month_input_data=="2020-02-01", "Less_than_1", "less_than_1")
data_hex_pt_w_composite<-data_hex_pt %>%
  mutate(
    idp_sites= ifelse(J.j2.idp_location=="informal_sites",1,0),
    IDP_present= ifelse(F.idp_now=="yes",1,0),
    IDP_time_arrive=  ifelse(F.f2.idp_time_arrive %in% c("1_month","3_months"),1,0),
    IDP_majority=  ifelse( F.f2.idp_perc %in% c("half","more_half"),1,0),
    food_inadequate= ifelse(G.food_now == "no", 1,0),
    less_one_meal = ifelse(G.meals_number %in% c("one", less_one_meal_var),1,0),
    hunger_severe_worse = ifelse(S.shock_hunger %in% c("hunger_severe", "hunger_worst"),1,0),
    wildfood_sick_alltime = ifelse(G.food_wild_emergency=="yes"|G.food_wild_proportion=="more_half",1,0),
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
            .funs = function(x){ifelse(analyzed_by_grid$State_id %in% valid_grids,x,NA)}) %>%
  st_drop_geometry()
analyzed_by_grid_filt %>% View()

if(output_aggregated_datasets=="yes"){
  write.csv(analyzed_by_grid_filt,hex_aggregated_monthly_data_output_file)
}

