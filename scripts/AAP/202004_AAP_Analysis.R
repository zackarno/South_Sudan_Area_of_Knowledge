
library(tidyverse)
library(sf)
library(lubridate)
library(srvyr)
source("scripts/functions/aok_aggregation_functions.R")

# LOAD IN DATA ------------------------------------------------------------
month_of_assessment<-"2020-02-01" #already in main script
# IF THE AOK PROCESS IS BUILT TO RUN SEQUENTIALLY WE DONT NECESSARILY HAVE TO LOAD THESE FROM THE DATA BECAUSE THEY WILL ALREADY BE IN MEMORY

aok_march <- read.csv("outputs/2020_03/aggregated_data/2020_04_21_reach_ssd_settlement_aggregated_AoK_Mar2020_Data.csv", stringsAsFactors = FALSE, na.strings=c(""," ")) %>% select(-X)
aok_feb<- read.csv("outputs/2020_02/aggregated_data/2020_04_21_reach_ssd_settlement_aggregated_AoK_Feb2020_Data.csv", stringsAsFactors = FALSE, na.strings=c(""," ")) %>% select(-X)


# This just investigates problems associated with LT data set -------------


aok_march %>% group_by(D.info_county, D.info_settlement) %>% mutate(asdf=n()) %>% filter(asdf>1)
aok_feb %>% group_by(D.info_county, D.info_settlement) %>% mutate(asdf=n()) %>% filter(asdf>1)
aok_march_lt %>% group_by(month,D.info_county, D.info_settlement) %>% mutate(asdf=n()) %>% filter(asdf>1) %>%
  arrange(D.info_settlement) %>% select(month)
aok_march_lt <- read.csv("outputs/2020_03/aggregated_data/2020_04_21_reach_ssd_settlement_aggregated_LONGTERM_AoK_Mar2020.csv", stringsAsFactors = FALSE, na.strings=c(""," ")) %>% select(-X)
aok_jan_lt<- "inputs/2020_02/REACH_SSD_AoK_LongTermSettlementData.csv"
aok_jan_lt<-read.csv(aok_jan_lt,stringsAsFactors = FALSE,
                             row.names = NULL, na.strings = c(""," ",NA, "NA","n/a"),
                             strip.white = TRUE)
aok_jan_lt %>% group_by(month,D.info_county, D.info_settlement) %>% mutate(asdf=n()) %>% filter(asdf>1) %>%
  arrange(D.info_settlement) %>% select(month)

# lets combine jan feb , but on duplicate settlements lets take th --------
aok_fm<-bind_rows(aok_feb, aok_march)


aok_fm %>% nrow()
aok_fm2<-aok_fm %>%
  mutate(month= month %>% ymd()) %>%
  group_by(D.info_county, D.info_settlement) %>%
  filter(month==max(month))
# aok_fm2 %>% write.csv('reach_ssd_AoK_FEB_MARCH_most_recent.csv')

st_layers("../../gis_data/gis_base/boundaries/county_shapefile")
adm2<- st_read(dsn="../../gis_data/gis_base/boundaries/county_shapefile","ssd_admbnda_adm2_imwg_nbs_20180401", stringsAsFactors=F ) %>% st_transform(crs=4326)
aok_fm2<- aok_fm2 %>%
  mutate(
    name_county_low=paste0(D.info_settlement,D.info_county) %>% butteR::tolower_rm_special())

mast_settlement<-read.csv("outputs/2020_03/settlement_lists/2020_03_working_master_settlement_list_2020_04_21.csv", stringsAsFactors = FALSE, na.strings=c(""," "))

#good quick check to make sure we have all settlementsin master list
aok_fm2 %>%
  filter(!name_county_low %in% mast_settlement$name_county_low) %>%
  select(D.info_state,D.info_county, D.info_settlement)


aok_with_coordinates<-aok_fm2 %>% left_join(mast_settlement %>% select(X,Y,name_county_low), by="name_county_low")
aok_sf<-st_as_sf(aok_with_coordinates,coords = c("X","Y"),crs=4326)

valid_counties<-mast_settlement %>%
  left_join(aok_fm2 %>% mutate(assessed=1) %>% select(name_county_low,assessed),by="name_county_low") %>%
  group_by(COUNTYJOIN) %>%
  summarise(total_assesed=sum(assessed,na.rm=T),
            total_settlements=n()) %>%
  mutate(percent_assessed= total_assesed/total_settlements) %>%
  filter(total_assesed>=0.05) %>%
  pull(COUNTYJOIN)
#HEX GRID HAS ALREADY BEEN CREATED
hex_grid <- st_read(dsn = "inputs/GIS",layer ="Grids_info", stringsAsFactors=F ) %>% st_transform(crs=4326) %>% select(-settlement)

hex_data_poly<-hex_grid %>% st_join(aok_sf)

data_hex_pt<- aok_sf %>% st_join(hex_grid)
data_hex_pt<-data_hex_pt %>% st_join(adm2)

grid_evaluation_table<-data_hex_pt %>%
  group_by(State_id) %>%
  summarise(num_setts_grid=n(),
            num_ki_grid=sum(D.ki_coverage))
valid_grids<-grid_evaluation_table %>%
  filter(num_setts_grid>1,num_ki_grid>1) %>%
  pull(State_id)

data_hex_pt<-data_hex_pt %>%
  mutate(
    i.cellphone= ifelse(R.phone_use %in% c("yes_network_often", "yes_network_sometimes"), "yes","no")
  )









cols_to_analyze<- c("i.cellphone")#, "Q.main_language","Q.language_pref_spoken","Q.language_pref_written")
aap_svy<-as_survey(data_hex_pt)
adm3_name
aap_svy %>%
  summarise_at(vars(starts_with("D.")),survey_mean)
aap_svy %>%
  group_by(starts_with("D.info_k"))
  summarise_at(survey_mean)

grid_quant_analysis<- butteR::mean_proportion_table(design =aap_svy,
                                          list_of_variables = cols_to_analyze,
                                          aggregation_level =  "State_id",
                                          na_replace = F)
county_quant_analysis<- butteR::mean_proportion_table(design =aap_svy,
                                          list_of_variables = cols_to_analyze,
                                          aggregation_level =  "admin2RefN",
                                          na_replace = F)

top_languages_chosen_by_grid<-data_hex_pt %>%
  group_by(State_id) %>%
  select(State_id, Q.main_language, Q.language_pref_spoken, Q.language_pref_written) %>%
  summarise_all(funs(aok_mode))




to_title_remove_unknowns<- function(x){
  ifelse(x %in% c("NC", "SL","dontknow", "other","Other","none","None"),NA,x) %>% str_to_title(.)
}

data_hex_pt_cleaned<- data_hex_pt %>%
  mutate_at(.vars= c("Q.main_language", "Q.language_pref_spoken", "Q.language_pref_written","Q.language_humanitarians"), .funs = to_title_remove_unknowns)


top_languages_chosen_by_county<-data_hex_pt_cleaned %>%
  group_by(admin2RefN) %>%
  select(admin2RefN, Q.main_language, Q.language_pref_spoken, Q.language_pref_written,Q.language_humanitarians) %>%
  summarise_all(funs(aok_mode)) %>%
  ungroup()

top_languages_chosen_by_county %>% filter(admin2RefN=='Maban')

data_hex_pt_cleaned %>% filter(admin2Name== "Maban" ) %>%
  group_by(Q.language_pref_written) %>% summarise(asdf=n())

# remove counties with < 5 % coverage
county_analysis_categ<- top_languages_chosen_by_county %>%
  mutate_at(.vars = c("Q.main_language", "Q.language_pref_spoken", "Q.language_pref_written"),
            .funs = function(x){ifelse(top_languages_chosen_by_county$admin2RefN %in% valid_counties,x,"insufficient_coverage")})

county_analysis_quant_to_map<-county_quant_analysis %>%
  select(admin2RefN,i.cellphone.yes,i.cellphone.no)


county_analysis_quant_to_map<-county_analysis_quant_to_map %>%
  mutate_at(.vars = c("i.cellphone.yes", "i.cellphone.no"),
            .funs = function(x) {ifelse(county_analysis_quant_to_map$admin2RefN %in% valid_counties, x,NA )})



grid_analysis_quant_to_map<-grid_quant_analysis %>%
  select(State_id,i.cellphone.yes,i.cellphone.no )

grid_analysis_quant_to_map<-grid_analysis_quant_to_map %>%
  mutate_at(.vars = c("i.cellphone.yes", "i.cellphone.no"),
            .funs = function(x) {ifelse(grid_analysis_quant_to_map$State_id %in% valid_grids, x,NA )})

adm2_w_analysis<-adm2 %>%
  left_join(county_analysis_categ %>% st_drop_geometry()) %>%
  left_join(county_analysis_quant_to_map)

hex_grid_w_analysis<-hex_grid %>% left_join(grid_analysis_quant_to_map)


# hex_grid_w_analysis %>%  st_write("qgis/AAP.gpkg",layer="AAP_2020_03_hex_febmarch_AoK", layer_options = 'OVERWRITE=YES', update = TRUE)
# adm2_w_analysis %>%  st_write("qgis/AAP.gpkg",layer="AAP_2020_03_county_febmarch_AoK2", layer_options = 'OVERWRITE=YES', update = TRUE)
aok_sf %>% select(D.info_state, D.info_county,D.info_settlement) %>%
  st_write("qgis/AAP.gpkg",layer="AAP_2020_03_settlement_feb_march", layer_options = 'OVERWRITE=YES', update = TRUE)



# top_3_langs_overall<-c("Q.main_language.dinka", "Q.main_language.nuer","Q.main_language.nuer _naath",
#                        "Q.language_pref_spoken.dinka",
#                        "Q.language_pref_spoken.dinka_jieng",
#                        "Q.language_pref_spoken.nuer",
#                        "Q.language_pref_written.english",
#                        "Q.language_pref_written.dinka",
#                        "Q.language_pref_written.nuer")







# grid_analysis_to_map$State_id<-grid_analysis_to_map$State_id %>% as.character()
grid_quant_analysis_long %>% nrow()
grid_quant_analysis_long<-grid_analysis_quant_to_map %>%
  pivot_longer(-State_id,
               names_to="indicator",
               values_to="indicator_val") %>%
  mutate(
    indicator_val_map= ifelse(!State_id %in% valid_grids,NA, indicator_val)
    )

top_languages_chosen<-data_hex_pt %>%
  group_by(State_id) %>%
  select(State_id, Q.main_language, Q.language_pref_spoken, Q.language_pref_written) %>%
  summarise_all(funs(aok_mode))
grid_categ_analysis_long<- top_languages_chosen %>% st_drop_geometry() %>%
  pivot_longer(-State_id,
               names_to="indicaor",
               values_to = "indicator_val")


# dir.create("outputs/2020_03/AAP")
# hex_grid %>% left_join(grid_quant_analysis_long) %>%  st_write("qgis/AAP.gpkg",layer="AAP_2020_03_covid_quant_indicators", layer_options = 'OVERWRITE=YES', update = TRUE)
# hex_grid %>% left_join(grid_categ_analysis_long) %>%  st_write("qgis/AAP.gpkg",layer="AAP_2020_03_covid_categ_indicators", layer_options = 'OVERWRITE=YES', update = TRUE)
# hex_grid %>% left_join(top_languages_chosen %>% st_drop_geometry()) %>%  st_write("qgis/AAP.gpkg",layer="AAP_2020_03_covid_categ_indicators_wide", layer_options = 'OVERWRITE=YES', update = TRUE)
adm2 %>% left_join()
# aok_sf %>%  st_write("qgis/AAP.gpkg",layer="AoK_March2020", layer_options = 'OVERWRITE=YES', update = TRUE)

# hex_grid %>% left_join(grid_categ_analysis_long)%>% st_write("outputs/2020_03/AAP/ssd_hex500_aap_cat.shp")
# st_write(strata_poly_long,"outputs/gis/bgd_covid2020.gpkg",layer="msna_ref_wash_2019_indicators_300m", layer_options = 'OVERWRITE=YES', update = TRUE)
grid_quant_analysis_long %>% filter(!is.na(indicator_val_map)) %>% nrow()
grid_quant_analysis_long %>% nrow()
grid_quant_analysis_long %>% filter(!is.na(indicator_val_map))

gquant<-grid_quant_analysis_long %>% filter(!is.na(indicator_val_map))
gquant %>% filter(is.na(indicator_val_map))


windows(); ggplot(gquant,
                  aes(x= indicator_val_map,
                      fill=cut(indicator_val,breaks=c(0,.02,.05,.10,.15,.20,.25,.30,.35,.40),include.lowest = T,right = T)),
                  na.rm=T)+
  geom_histogram(bins=100,na.rm = TRUE)+
  facet_wrap(facets = ~indicator)


adm2_gdb<-"../../gis_data/gis_base/boundaries/county_shapefile"
adm2<- st_read(adm2_gdb,"ssd_admbnda_adm2_imwg_nbs_20180401")
# st_write(grid_poly_joined,"qgis/aap/shp/feb_aok_aap_hex_analysis.shp")
st_layers(adm2_gdb)[1]
library(tmap)
?tm_polygons
tm_shape(grid_poly_joined)+
  # tm_fill(showNA=F )
  tm_polygons("i.cellphone.yes", border.col = NA)+

tm_shape(adm2)+
  tm_polygons(col=NA, alpha=0,border.col = "black")


