#' @import lubridate 
#' @import tidyr 
#' @import readr 
#' @import tidyjson 
#' @import jsonlite 
#' @importFrom dplyr select mutate filter group_by collect distinct count
NULL

#' Function that reads in the raw data, extracts the payloads then converts it into json format
#' The function name is json_sensorraw()
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

#' Function that  extracts the location probe from the payloads and converts to json format
#' The function name is json_probelocation()
#' @export
json_probelocation<-function(pl){
  
  probe_location<-function(pl){
    
    filterPayload <- function(pl, probeFilter) {
      pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
    }
    
    locationProbe <- pl %>% filterPayload("\\.LocationProbe$") %>% 
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
  return(toJSON(probe_location(pl)))
}

#' Function that  extracts the sms probe from the payloads and converts to json format
#' The function name is json_probesms()
#' @export

json_probesms<-function(pl){
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
  return(toJSON(probe_sms(pl)))
}

#' Function that  extracts the call log probe from the payloads and converts them to json format
#' The function name is json_probecalls()
#' @export
#' 

json_probecalls<-function(pl){
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
  return(toJSON(probe_calls(pl)))
}

#' Function that  extracts the communication event probe from the payloads and converts to 
#' json format.
#' The function name is json_probeevent()
#' @export
#' 
json_probeevent<-function(pl){
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
  return(toJSON(probe_event(pl)))
}

#' Function that  displays a table for available data in each probe in json format
#' The function name is json_sensorpop()
#' @export
#' 
json_sensorpop<-function(pl){
  sensor_population<-function(pl,sample_list){
    filterPayload <- function(pl, probeFilter) {
      pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
    }
    locationProbe <- pl %>% filterPayload("\\.LocationProbe$") %>% 
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
    
    SmsMessages <- pl %>% filterPayload("\\.CommunicationLogProbe$")%>%
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
    
    PhoneCalls <- pl %>% filterPayload("\\.CommunicationLogProbe$")%>%
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
    
    Event <- pl %>% filterPayload(probeFilter) %>% 
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
    
    unique_registered<-sample_list%>%
      filter(!duplicated(user_id))%>%
      mutate(location=ifelse(user_id%in%unique(locationProbe$user_id),"Available","NotAvailable"),
             calls=ifelse(user_id%in%unique(PhoneCalls$user_id),"Available","NotAvailable"),
             sms=ifelse(user_id%in%unique( SmsMessages$user_id),"Available","NotAvailable"),
             event =ifelse(user_id%in%unique(Event$user_id),"Available","NotAvailable"))%>%
      select(-email)
    return(unique_registered)
  }
  return(toJSON(sensor_population(pl,sample_list)))
}

#' Function that displays the count for each communication type, Phones and SMS and converts
#' it into json format.
#' The function name is json_commtype()
#' @export
json_commtype<-function(pl){
  count_commtype<- function(pl){
    filterPayload <- function(pl, probeFilter) {
      pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
    }
    probeFilter <- "\\.CommunicationEventProbe$"
    commtype_count <- pl %>% filterPayload(probeFilter) %>% 
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
             event_date = as.Date(event_timestamp))%>% 
      group_by(user_id,event_date)%>%
      count(communication_type)%>%
      spread(communication_type, n)
    return(commtype_count)
  }
  
  return(toJSON(count_commtype(pl)))
}


#' Function that counts the daily number, and average number of incoming, outgoing and  missed calls per
#' The function name is json_statsphone()
#' @export
json_statsphone<-function(pl){
  stats_phone<-function(pl){
    filterPayload <- function(pl, probeFilter) {
      pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
    }
    probeFilter <- "\\.CommunicationEventProbe$"
    phones_daily <- pl %>% filterPayload(probeFilter) %>% 
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
             event_date = as.Date(event_timestamp))%>% 
      filter(communication_type == "PHONE")%>%
      group_by(user_id,event_date)%>%
      count(comm_direction)%>%
      mutate(average_no.calls = ceiling(mean(n)))%>%
      spread(comm_direction, n)
    return(phones_daily)
  } 
  return(toJSON(stats_phone(pl)))
}

#' Function that counts the daily number, and average number of incoming and outgoing sms messages per user
#' The function name is json_statssms()
#' @export
json_statssms<-function(pl){
  stats_sms<-function(pl){
    filterPayload <- function(pl, probeFilter) {
      pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
    }
    probeFilter <- "\\.CommunicationEventProbe$"
    sms_daily<- pl %>% filterPayload(probeFilter) %>% 
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
             event_date = as.Date(event_timestamp))%>% 
      filter(communication_type == "SMS")%>%
      group_by(user_id,event_date)%>%
      count(comm_direction)%>%
      mutate(average_no.messages = ceiling(mean(n)))%>%
      spread(comm_direction, n)
    return(sms_daily)
  }
  return(toJSON(stats_sms(pl)))
}

#' Function that displays the ratio of outgoing calls to incoming calls.
#' The function name is json_ratiophone()
#' @export

json_ratiophone<-function(pl){
  ratio_phone<-function(pl){
    filterPayload <- function(pl, probeFilter) {
      pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
    }
    probeFilter <- "\\.CommunicationEventProbe$"
    phone_ratio<-pl %>% filterPayload(probeFilter) %>% 
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
             event_date = as.Date(event_timestamp))%>% 
      filter(communication_type == "PHONE")%>%
      group_by(user_id,event_date)%>%
      count(comm_direction)%>%
      spread(comm_direction, n)%>%
      mutate(ratio= OUTGOING / INCOMING)  
    return(phone_ratio)
  }
  
  return(toJSON(ratio_phone(pl)))
}

#' Function that displays the ratio of outgoing texts to incoming texts.
#' The function name is json_ratiosms()
#' @export
json_ratiosms<-function(pl){
  ratio_sms<-function(pl){
    filterPayload <- function(pl, probeFilter) {
      pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
    }
    probeFilter <- "\\.CommunicationEventProbe$"
    sms_ratio<-pl %>% filterPayload(probeFilter) %>% 
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
             event_date = as.Date(event_timestamp))%>% 
      filter(communication_type == "SMS")%>%
      group_by(user_id,event_date)%>%
      count(comm_direction)%>%
      spread(comm_direction, n)%>%
      mutate(ratio= OUTGOING / INCOMING)  
    return(sms_ratio)
  }
  return(toJSON(ratio_sms(pl)))
}

#' Function that displays the time of day that each user has the maximum call duration
#' The function name is json_callduration()
#' @export
json_callduration<-function(pl){
  call_duration<-function(pl){
    filterPayload <- function(pl, probeFilter) {
      pl%>% filter(grepl(probeFilter, probe))%>%select(-probe)
    }
    max_callduration<-pl %>% filterPayload("\\.CommunicationLogProbe$")%>%
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
             event_date = as.Date(call_timestamp))%>%
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
  
  return(toJSON(call_duration(pl)))
}

#' Function that displays the number of unique contacts texted every day by each user
#' The function name is json_uniquetext()
#' @export

json_uniquetext<-function(pl){
  unique_texts<-function(pl){
    filterPayload <- function(pl, probeFilter) {
      pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
    }
    probeFilter <- "\\.CommunicationEventProbe$"
    unique_textnumbers<- pl %>% filterPayload(probeFilter) %>% 
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
             event_date = as.Date(event_timestamp))%>%
      filter(communication_type == "SMS")%>%
      group_by(user_id,event_date)%>%
      mutate(count_unique=length(unique(number)))%>%
      select(user_id,number, event_date,count_unique)
    return(unique_textnumbers)
  }
  
  return(toJSON(unique_texts(pl)))
}

#' Function that displays the number of unique contacts called every day by each user
#' The function name is json_uniquecalls()
#' @export
json_uniquecalls<-function(pl){
  unique_calls<-function(pl){
    filterPayload <- function(pl, probeFilter) {
      pl %>% filter(grepl(probeFilter, probe))%>%select(-probe)
    }
    probeFilter <- "\\.CommunicationEventProbe$"
    unique_callnumbers<- pl %>% filterPayload(probeFilter) %>% 
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
             event_date = as.Date(event_timestamp))%>%
      filter(communication_type == "PHONE")%>%
      group_by(user_id,event_date)%>%
      mutate(count_unique=length(unique(number)))%>%
      select(user_id,number, event_date,count_unique)
    return(unique_callnumbers)
  }
  return(toJSON(unique_calls(pl)))
}
