library(tidyverse)
library(butteR)
library(koboloadeR)
library(lubridate)
library(sf)

source("scripts/functions/aok_aggregation_functions.R")
source("scripts/functions/aok_cleaning_functions.R")
source("scripts/functions/aggregate_aok_by_settlement.R")
iso_date<- Sys.Date() %>%  str_replace_all("-","_")
admin_gdb<- "../../gis_data/gis_base/boundaries/county_shapefile"
latest_settlement_data <- "outputs/2020_02/2020_02_master_settlement_list_2020_04_15.csv"



master_settlement<-read.csv(latest_settlement_data, stringsAsFactors = FALSE)
colnames(master_settlement)<-paste0("mast.",colnames(master_settlement))
master_settlement_sf<- st_as_sf(master_settlement,coords=c("mast.X","mast.Y"), crs=4326)
month_of_assessment<-"2020-03-01"
month_label<-month(month_of_assessment,label = T)
cl_issue_file_name<- paste0(iso_date,"_AoK_CL_issues_",month_label,"2020_Data.csv")
# LOAD RAW DATA -----------------------------------------------------------

aok_raw_list <-butteR::read_all_csvs_in_folder(input_csv_folder = "inputs2/2020_03/raw_data/")
purrr::map(aok_raw_list,nrow)

aok_raw<- dplyr::bind_rows(aok_raw_list)
sm_cols_df<-butteR::extract_sm_option_columns(df = aok_raw,name_vector=colnames(aok_raw))

fix_logical_to_yn<-function(x){
  x %>% as.character() %>% str_replace_all(c("1"="yes","0"="no"))
}

aok_raw<-aok_raw %>%
  mutate_at(.vars= sm_cols_df$sm_options,.funs = fix_logical_to_yn)


aok_raw %>% nrow()
#making sure all our uuid in the dataset are unique

aok_raw <- distinct(aok_raw,X_uuid, .keep_all= TRUE)

# STEP 1 COMPILE CLEANING LOGS --------------------------------------------
cleaning_log_converted_to_csv<-c("yes","no")[2]
if(cleaning_log_converted_to_csv=="yes"){
  cleaning_log_path <- "inputs2/2020_03/cleaning_logs"
  cleaning_logs<-butteR::read_all_csvs_in_folder(input_csv_folder = cleaning_log_path)
  cleaning_log<-bind_rows(cleaning_logs)
}
if(cleaning_log_converted_to_csv=="no"){
  xlsx_dir<-"inputs2/2020_03/cleaning_logs"
  xlsx_files<-list.files(xlsx_dir, full.names = T)
  xlsx_file_names<-list.files(xlsx_dir, full.names = F)
  cleaning_logs<-list()
  for(i in 1: length(xlsx_files)){
    file_name_temp<- xlsx_file_names[i]
    path_temp<-xlsx_files[i]
    print(file_name_temp)
    df<-readxl::read_xlsx(path = path_temp, sheet = "cleaning_log")
    cleaning_logs[[file_name_temp]]<-df
  }
  cleaning_log<-bind_rows(cleaning_logs)}


num_uuid_in_cl<-cleaning_log %>% select(contains("uid")) %>% colnames() %>% length()
if(num_uuid_in_cl>1){
  print(cleaning_log %>% select(contains("uid")) %>% colnames() )
}

# cleaning_log<-cleaning_log %>%
#   mutate(uuid=ifelse(is.na(uuid),ï..uuid, uuid)) %>%
#   select(-ï..uuid)

cleaning_log<- cleaning_log %>% left_join(aok_raw %>%
                                            select(X_uuid,A.base ), by=c("uuid"="X_uuid"))

cleaning_log_list<-list()
cleaning_log_list[["main"]]<-cleaning_log
# cleaning_log %>%
  # filter(str_detect(indicator,"settlement")) %>% View()

cleaning_log_actionable<- cleaning_log %>% filter(change_type!="no_action")

# CHECK CLEANING LOG

aok_cleaning_checks<-butteR::check_cleaning_log(df = aok_raw, df_uuid = "X_uuid",
                           cl = cleaning_log_actionable,
                           cl_change_type_col = "change_type",
                           cl_change_col = "indicator",
                           cl_uuid = "uuid",
                           cl_new_val = "new_value")

cleaning_log_actionable<- cleaning_log_actionable %>%
  filter(!uuid%in%aok_cleaning_checks$uuid)

aok_cleaning_checks<-butteR::check_cleaning_log(df = aok_raw, df_uuid = "X_uuid",
                                                cl = cleaning_log_actionable,
                                                cl_change_type_col = "change_type",
                                                cl_change_col = "indicator",
                                                cl_uuid = "uuid",
                                                cl_new_val = "new_value")

# dir.create("outputs/2020_03/cleaning_log_issues", recursive = T)
# aok_cleaning_checks %>% write.csv(paste0("outputs/2020_03/cleaning_log_issues/",cl_issue_file_name))
cleaning_log_actionable$change_type %>% unique()
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

aok_clean%>%
  filter(!is.na(D.info_settlement_other)) %>%
  select(D.info_settlement_other) %>%
  arrange(D.info_settlement_other)


# OUTPUT ISSUES TO AOS

# IMPLEMENT CLEANING LOG

# CREATE AOK_CLEAN
# STEP 2 NEW SETTLEMENTS --------------------------------------------------
# PERHAPS THE MOST DIFFICULT STEP
# OUTPUTS: NEW ITEMSET, NEW MASTER SETTLEMENT FILE, CLEANING LOG


new_settlements<- butteR::read_all_csvs_in_folder("inputs2/2020_03/new_settlements")
new_sett<- bind_rows(new_settlements)
new_sett<-new_sett %>%
  mutate(
    D.info_settlement_other=ifelse(is.na(D.info_settlement_other),New.settlements,D.info_settlement_other)
    )



# new_sett<-new_sett %>%
#   mutate(uuid=ï..uuid) %>%
#   filter(action=="Map") %>%
#     select(-ï..uuid)



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


adm2<- st_read(admin_gdb,"ssd_admbnda_adm2_imwg_nbs_20180401" )
adm2<-st_transform(adm2,crs=4326)
new_sett3<-new_sett2 %>%
  filter(!is.na(longitude)& !is.na(latitude))

new_sett_sf<-st_as_sf(new_sett3,coords=c("longitude","latitude"), crs=4326)


#this is a good check to see if points fall out of SSD
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
# aok_clean1<-aok_raw
aok_clean
# aok_clean1[aok_clean1$X_uuid=="b4d97108-3f34-415d-9346-f22d2aa719ea","D.info_settlement_other"]<-NA
# aok_clean1[aok_clean1$X_uuid=="b4d97108-3f34-415d-9346-f22d2aa719ea","D.info_settlement"]<-"Bajur"


remove_from_new_sett<-aok_clean %>%
  filter(X_uuid %in% new_sett_sf$uuid  & is.na(D.info_settlement_other))%>%
  select(X_uuid,D.info_settlement) %>% pull(X_uuid)



aok_other_not_in_settlement_data<-aok_clean %>%
  filter(!is.na(D.info_settlement_other)) %>%
  filter(!X_uuid %in% new_sett_sf$uuid) %>%
  select(X_uuid,A.base,D.info_settlement, D.info_settlement_other) %>%
  arrange(A.base) # %>%
# aok_other_not_in_settlement_data %>% write.csv("outputs/2020_03/cleaning_log_issues/MAR2020_AoK_other_settlements_not_CL_or_new_settlements.csv")

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
cleaning_log_actionable %>% filter(str_detect(indicator, "settlement")) %>% View()



# NEW SETTLEMENT DATA WHICH MATCHES MASTER SETTLEMENTS EXACTLY ------------

exact_matches1<-new_sett_sf %>%
  mutate(matched_where= case_when(new.enum_sett_county %in% master_settlement_sf$mast.settlement_county_sanitized~"enum", #CHECK WITH ENUMS INPUT
                                  new.adm2_sett_county %in% master_settlement_sf$mast.settlement_county_sanitized~"shapefile_only" #CHECK WITH SHAEFILE COUNTY
  )) %>%
  filter(!is.na(matched_where)) #ONLY RETURN EXACT MATCHES

# WRITE EXACT MATCHES TO CLEANING LOG TO THEN IMPLEMENT ON DATA.

# debugonce(exact_matches_to_cl)
aok_exact_matches_cl<-exact_matches_to_cl(exact_match_data = exact_matches1,
                                          user = "Zack",
                                          uuid_col = "uuid",
                                          settlement_col = "name_new_settlement")

# aok_exact_matches_cl %>% write.csv("outputs/2020_03/cleaning_log_issues/new_settlements_in_kobo_data_that_already_exist_AoK_MAR_Data.csv")

#NOW IMPLEMENT THIS CLEANING LOG!

aok_exact_matches_cl %>%
  filter(!uuid %in% aok_clean$X_uuid)

aok_exact_matches_cl %>% nrow()
aok_exact_matches_cl_filt<-aok_exact_matches_cl %>%
  filter(uuid %in% aok_clean$X_uuid)
cleaning_log_list[["auto_gen_exact_matches"]]<- aok_exact_matches_cl_filt

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
new_settlement_evaluation<-evaluate_unmatched_settlements(user= "zack",new_settlement_table = settlements_best_guess, uuid_col="uuid")

master_settlement %>% filter( str_detect(mast.NAME, "salam"))

new_settlement_evaluation$checked_setlements
cleaning_log_list[["auto_gen_settlements_evaluated"]]<- new_settlement_evaluation$cleaning_log
# write.csv(new_settlement_evaluation$checked_setlements,"outputs/2020_03/2020_03_evaluated_settlement_log.csv")
# write.csv(new_settlement_evaluation$cleaning_log,"outputs/2020_03/2020_03_evaluated_settlement_cleaning_log.csv")



new_settlement_evaluation$cleaning_log

# debugonce(implement_cleaning_log)
aok_clean3<-butteR::implement_cleaning_log(df = aok_clean2,df_uuid = "X_uuid",
                                           cl =new_settlement_evaluation$cleaning_log ,
                                           cl_change_type_col = "change_type",
                                           cl_change_col = "suggested_indicator",
                                           cl_uuid = "uuid",
                                           cl_new_val = "suggested_new_value")


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
itemset<-read.csv("outputs/2020_02/2020_02_itemset_updated2020_04_15.csv", strip.white = T, stringsAsFactors = T,na.strings = c(" ",""))
itemset_not_other<-itemset %>% filter(name!="other")
itemset_other<- itemset %>% filter(name=="other")


itemset_binded<-bind_rows(list(new_sets_to_add_itemset,itemset_not_other)) %>% arrange(admin_2)
itemset_full_binded<- bind_rows(list(itemset_binded,itemset_other))
itemset_file_name<- paste0("2020_03_itemset_updated_",iso_date,".csv")
#write to csv for next aok round
write.csv(itemset_full_binded,paste0("outputs/2020_03/",itemset_file_name))

# NEXT WE ADD THE NEW SETTLEMENTS TO THE SHAPEFILE ------------------------

# add to master file ------------------------------------------------------
# new_sett<-read.csv("inputs/new_settlements/20200207_New_settlement_Jan2020_ZA.csv", stringsAsFactors = FALSE)
# new_sett %>% head()
master_settlement<-read.csv(latest_settlement_data, stringsAsFactors = FALSE)
# master_settlement$DATA_SOURC
# new_sett_sf
new_settlement_evaluation$checked_setlements$new.D.info_settlement_other
new_setts_add_to_master<-new_settlement_evaluation$checked_setlements %>%
  filter(action==2) %>%
  mutate(
    NAME= new.D.info_settlement_other %>%  gsub("'","",.) %>% gsub(" ","-", .),
    NAMEJOIN= NAME  %>% gsub("-","_",.),
    NAMECOUNTY=paste0(NAMEJOIN,new.county_adm2),
    COUNTYJOIN= new.county_adm2 ,
    DATE= month_of_assessment %>% ymd(),
    DATA_SOURC="AOK",
    IMG_VERIFD= 0
  ) %>% #get coordinates from field data back in
  left_join(new_sett_sf %>%
              st_drop_geometry_keep_coords(), by="uuid") %>%
  filter(!is.na(X)) %>%
  select(NAME,NAMEJOIN,NAMECOUNTY,COUNTYJOIN,DATE,DATA_SOURC,IMG_VERIFD,X,Y)

# gsub("[[:punct:]]","",.)
master_new<-bind_rows(list(new_setts_add_to_master,master_settlement %>% mutate(DATE=dmy(DATE))))
master_new_file_name<- paste0("2020_03_master_settlement_list_",iso_date,".csv")
# write.csv(master_new,paste0("outputs/2020_03/",master_new_file_name))


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



#maybe change the way assessment month is represented

# month_label<-month(month_of_assessment,label = T)
aggregated_file_name<- paste0(
  month_label,"2020_AoKData_SSD_settlement_aggregated_",iso_date,".csv")
aggregated_long_term_file_name<- paste0(
  month_label,"2020_AoKData_LONGTERM_SSD_settlement_aggregated_",iso_date,".csv")

#next start with the rmeove grouper stuff.
# prev_round<-read.csv("inputs/2020_01/2020_02_13_reach_ssd_aok_clean_data_compiled.csv", stringsAsFactors = FALSE, na.strings=c("", " ", NA, "NA"))
prev_round<-read.csv("outputs/2020_04_17_reach_ssd_settlement_aggregated_AoK_Feb2020_Data.csv", stringsAsFactors = FALSE, na.strings=c("", " ", NA, "NA"))
# debugonce(aggregate_aok_by_county)


aok_clean_aggregated_settlement<-aggregate_aok_by_settlement(clean_aok_data = aok_clean4,aok_previous = prev_round, current_month = month_of_assessment)



long_term_aok_latest_file<- "outputs/2020_04_17_reach_ssd_settlement_aggregated_LONGTERM_AoK_Feb2020.csv"
previous_long_term<-read.csv(long_term_aok_latest_file,stringsAsFactors = FALSE,
         row.names = NULL, na.strings = c(""," ",NA, "NA","n/a"),
         strip.white = TRUE)


colnames(previous_long_term)[!colnames(previous_long_term)%in% colnames(aok_clean_aggregated_settlement)]
colnames(aok_clean_aggregated_settlement)[!colnames(aok_clean_aggregated_settlement)%in% colnames(previous_long_term)]
nrow(previous_long_term)
previous_long_term %>% filter(!is.na(L.ag_inputs)) %>% nrow()
previous_long_term$L.ag_inputs

aok_raw$L.current_activities.salaries


check_in_data<-c("G.food_now_type.Meat", "G.food_now_type.Vegetables", "G.food_now_type.Main_staples",
  "G.food_now_type.Fruit", "G.food_now_type.Pulses", "G.food_now_type.Milk_Dairy",
  "L.current_activities.Salaries", "G.food_coping_comsumption.none",
  "G.food_coping_comsumption.limit_meal_size", "G.food_coping_comsumption.only_children_eat"
)
previous_long_term<- previous_long_term %>% rename_at(vars(check_in_data),funs(stringr::str_to_title(check_in_data)))

long_term_aggregated_new<-bind_rows(previous_long_term, aok_clean_aggregated_settlement)

long_term_aggregated_new[,check_in_data %>% stringr::str_to_title()]
months_2020<-c("2020-02-01", "2020-03-01")
long_term_aggregated_new$month %>% unique() %>% dput()
long_term_aggregated_new[,c("month",check_in_data%>% stringr::str_to_title())] %>%
  filter(month %in% months_2020[1])
aok_raw %>% select(contains("food_now_type.")) %>% colnames() %>% dput()
aok_clean_aggregated_settlement$G.food_now_type.dontknow
long_term_aggregated_new$G.food_now_type.Vegetables
aok_raw$G.food_now_type.vegetables
# dir.create("outputs/2020_03/aggregated_data")
# write.csv(aok_clean_aggregated_settlement,paste0("outputs/2020_03/aggregated_data/", aggregated_file_name), na="")
# write.csv( long_term_aggregated_new,paste0("outputs/2020_03/aggregated_data/",aggregated_long_term_file_name),na = "")


# march_data<- read.csv("outputs/2020_03/aggregated_data/Mar2020_AoKData_SSD_settlement_aggregated_2020_04_16.csv",,stringsAsFactors = FALSE,
         # row.names = NULL, na.strings = c(""," ",NA, "NA","n/a"),
         # strip.white = TRUE)
# long_term_aggregated_new<-bind_rows(previous_long_term, march_data)
# long_term_aggregated_new$L.current_activities.crops_for_sustenance
# write.csv( long_term_aggregated_new,paste0("outputs/2020_03/aggregated_data/",aggregated_long_term_file_name),na = "")

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









