#' @import lubridate 
#' @import tidyr 
#' @import readr 
#' @import tidyjson 
#' @import jsonlite
#' @rawNamespace
#' if (utils::packageVersion("dplyr") >= "0.5.0") {
#'   importFrom("dplyr")
#' } 
NULL

#' Function that reads in the raw data and extracts the payloads.
#' The function name is sensor_raw()
#' @export
#' 
sensor_raw<-function(raw_data){
  pl <- raw_data %>% select(user_id, payload) %>% collect %>% 
    mutate(payload = paste("[", payload, "]", sep = "")) %>% 
    as.tbl_json(.,json.column="payload") %>% gather_array  %>% 
    spread_values(
      probe=jstring("PROBE"),
      timestamp=jnumber("TIMESTAMP"),
      guid=jstring("GUID"))
  (plTypes <- pl %>% distinct(probe)) 
  return(pl)
}

#' Function that  extracts the location probe from the payloads.
#' The function name is probe_location()
#' @export
#' 
probe_location<-function(pl){
  filterPayload <- function(pl, probeFilter) {
    pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
  }
  probeFilter <- "\\.LocationProbe$"
  tryCatch({
    pl %>% sampleProbe(probeFilter) 
  }, error=function(e){
    print("LocationSensorProbe data is not available")
  })
  locationProbe <- pl %>% filterPayload(probeFilter) %>% 
    spread_values(
      gpsAvailable=jlogical("GPS_AVAILABLE"),
      latitude=jnumber("LATITUDE"),
      timeFix=jstring("TIME_FIX"),
      longitude=jnumber("LONGITUDE"),
      networkAvailable=jlogical("NETWORK_AVAILABLE"),
      accuracy=jnumber("ACCURACY"),
      provider=jstring("NETWORK"))%>% 
    select(user_id, timestamp, gpsAvailable, latitude, longitude, networkAvailable, accuracy) %>%                                 mutate(event_timestamp = as.POSIXct(timestamp, origin="1970-01-01")) %>% 
    mutate(event_hour = hour(event_timestamp),event_date = as.Date(event_timestamp)) 
  
  return(locationProbe)
}

#' Function that  extracts the sms probe from the payloads.
#' The function name is probe_sms()
#' @export
#' 
probe_sms<-function(pl){
  
  filterPayload <- function(pl, probeFilter) {
    pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
  }
  communicationLogProbesSmsMessages <- pl %>% filterPayload("\\.CommunicationLogProbe$")%>%
    enter_object("SMS_MESSAGES") %>% gather_array %>%
    spread_values(
      numberName=jstring("NUMBER_NAME"),
      normalizedHash=jstring("NORMALIZED_HASH"),
      messageDirection=jstring("MESSAGE_DIRECTION"),
      number=jstring("NUMBER"),
      messageTimestamp=jnumber("MESSAGE_TIMESTAMP")
    ) %>% select(-array.index)%>% 
    select(user_id, numberName, messageDirection, number, messageTimestamp) %>% 
    mutate(msg_timestamp = as.POSIXct(messageTimestamp/1000, origin="1970-01-01")) %>%
    mutate(event_hour = hour(msg_timestamp),
           event_date = as.Date(msg_timestamp))
  return(communicationLogProbesSmsMessages)
}

#' Function that  extracts the call log probe from the payloads.
#' The function name is probe_calls()
#' @export
#' 
probe_calls<-function(pl){
  
  filterPayload <- function(pl, probeFilter) {
    pl%>% filter(grepl(probeFilter, probe))%>%select(-probe)
  }
  
  communicationLogProbesPhoneCalls <- pl %>% filterPayload("\\.CommunicationLogProbe$")%>%
    enter_object("PHONE_CALLS") %>% gather_array %>%
    spread_values(
      numberName=jstring("NUMBER_NAME"),
      callDuration=jnumber("CALL_DURATION"),
      numberLabel=jstring("NUMBER_LABEL"),
      normalizedHash=jstring("NORMALIZED_HASH"),
      numberType=jstring("NUMBER_TYPE"),
      callTimestamp=jnumber("CALL_TIMESTAMP"),
      number=jstring("NUMBER")
    ) %>% select(-array.index)%>% 
    select(user_id, numberName, callDuration, numberLabel, numberType, number, callTimestamp) %>% 
    mutate(call_timestamp = as.POSIXct(callTimestamp/1000, origin="1970-01-01")) %>%
    mutate(event_hour = hour(call_timestamp),
           event_date = as.Date(call_timestamp))
  return(communicationLogProbesPhoneCalls)
}

#' Function that  extracts the communication event probe from the payloads.
#' The function name is probe_event()
#' @export

probe_event<-function(pl){
  filterPayload <- function(pl, probeFilter) {
    pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
  }
  probeFilter <- "\\.CommunicationEventProbe$"
  CommunicationEventProbe <- pl %>% filterPayload(probeFilter) %>% 
    spread_values(
      name=jstring("NAME"),
      timestamp=jnumber("TIMESTAMP"),
      comm_timestamp=jstring("COMM_TIMESTAMP"),
      probe=jstring("PROBE"),
      comm_direction=jstring("COMMUNICATION_DIRECTION"),
      number=jstring("NUMBER"),
      communication_type=jstring("COMMUNICATION_TYPE")
    )%>% 
    select(user_id, comm_timestamp, comm_direction, number, communication_type) %>% 
    mutate(event_timestamp = as.POSIXct(as.numeric(comm_timestamp)/1000, origin="1970-01-01")) %>%
    mutate(event_hour = hour(event_timestamp),
           event_date = as.Date(event_timestamp))
  return(CommunicationEventProbe)
}


#' Function that  displays a table for available data in each probe.
#' The function name is sensor_population()
#' @export
#' 
sensor_population<-function(master_data,location_sensor,calls_sensor,sms_sensor,commevent_sensor ){
  unique_registered<-master_data%>%
    filter(!duplicated(user_id))%>%
    mutate(location=ifelse(user_id%in%unique(location_sensor$user_id),"Available","NotAvailable"),
           calls=ifelse(user_id%in%unique(calls_sensor$user_id),"Available","NotAvailable"),
           sms=ifelse(user_id%in%unique(sms_sensor$user_id),"Available","NotAvailable"),
           event =ifelse(user_id%in%unique(commevent_sensor$user_id),"Available","NotAvailable"))%>%
    select(-email)
  return(unique_registered)
}

#' Function that displays the count for each communication type, Phones and SMS.
#' The function name is count_commtype()
#' @export
count_commtype<- function(df){
  commtype_count<-df%>% 
    group_by(user_id,event_date)%>%
    count(communication_type)%>%
    spread(communication_type, n)
  return(commtype_count)
}

#' Function that counts the daily number, and average number of incoming, outgoing and  missed calls per
#' The function name is stats_phone()
#' @export

stats_phone<-function(df){
  phones_daily<- df %>% 
    filter(communication_type == "PHONE")%>%
    group_by(user_id,event_date)%>%
    count(comm_direction)%>%
    mutate(average_no.calls = ceiling(mean(n)))%>%
    spread(comm_direction, n)
  
  return(phones_daily)
}

#' Function that counts the daily number, and average number of incoming and outgoing sms messages per user
#' The function name is stats_sms()
#' @export

stats_sms<-function(df){
  sms_daily<- df %>% 
    filter(communication_type == "SMS")%>%
    group_by(user_id,event_date)%>%
    count(comm_direction)%>%
    mutate(average_no.messages = ceiling(mean(n)))%>%
    spread(comm_direction, n)
  
  return(sms_daily)
}

#' Function that displays the ratio of outgoing calls to incoming calls.
#' The function name is ratio_phone()
#' @export

ratio_phone<-function(df){
  phone_ratio<-df %>% 
    filter(communication_type == "PHONE")%>%
    group_by(user_id,event_date)%>%
    count(comm_direction)%>%
    spread(comm_direction, n)%>%
    mutate(ratio= OUTGOING / INCOMING)  
  return(phone_ratio)
}

#' Function that displays the ratio of outgoing texts to incoming texts.
#' The function name is ratio_sms()
#' @export

ratio_sms<-function(df){
  sms_ratio<-df %>% 
    filter(communication_type == "SMS")%>%
    group_by(user_id,event_date)%>%
    count(comm_direction)%>%
    spread(comm_direction, n)%>%
    mutate(ratio= OUTGOING / INCOMING)  
  return(sms_ratio)
}

#' Function that displays the time of day that each user has the maximum call duration
#' The function name is call_duration()
#' @export

call_duration<-function(call_sensor){
  max_callduration<-calls_sensor%>%
    group_by(user_id, event_date)%>%
    mutate(time_of_day =ifelse(event_hour>=3 & event_hour<6,"Early Morning",
                               ifelse(event_hour>=6 & event_hour<12, "Morning",
                                      ifelse(event_hour>=12 & event_hour<19,"Afternoon",
                                             ifelse(event_hour>=19 & event_hour<22,"Night","Late Night"))))
           ,max_duration = max(callDuration))%>%
    mutate(max_time =ifelse(callDuration== max(callDuration),"max_time",0))%>%
    select(user_id, call_timestamp,event_date, callDuration, max_time, time_of_day)%>%
    arrange(call_timestamp)%>%
    filter(max_time == "max_time")
  return(max_callduration)
}

#' Function that displays the number of unique contacts texted every day by each user
#' The function name is unique_texts()
#' @export
unique_texts<-function(commevent){
  unique_textnumbers<-commevent %>% 
    filter(communication_type == "SMS")%>%
    group_by(user_id,event_date)%>%
    mutate(count_unique=length(unique(number)))%>%
    select(user_id,number, event_date,count_unique)
  return(unique_textnumbers)
}

#' Function that displays the number of unique contacts called every day by each user
#' The function name is unique_calls()
#' @export
unique_calls<-function(commevent){
  unique_callnumbers<-commevent %>% 
    filter(communication_type == "PHONE")%>%
    group_by(user_id,event_date)%>%
    mutate(count_unique=length(unique(number)))%>%
    select(user_id,number, event_date,count_unique)
  return(unique_callnumbers)
}





