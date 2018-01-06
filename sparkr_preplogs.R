
library(ndjson)
library(dplyr)
library(lubridate)

#set the logFile on line 8
#set the correct col names for id, timestamp, latitude, longitude on line 18

logFile <- "/home/russel_ebizu/clustergps/part6.log"

logPath <- "/home/rstudio" #dirname(logFile)
logName <- gsub("\\.","_", basename(logFile))

message("Reading in logfile ", date())
samplog <- stream_in(logFile)

message("Selecting only relevant cols ", date())
samplog <- select(samplog, id, timestamp, latitude, longitude)

names(samplog) <- c("id", "timestamp", "latitude", "longitude")

message("Converting timestamp to datetime ", date())
samplog$timestamp <- as.POSIXct(samplog$timestamp/1000,origin='1970-01-01')

message("Populating hr column ", date())
samplog <- mutate(samplog, hr = hour(timestamp))

n <- nrow(samplog)

k <- ceiling(n/1e6)

if(!dir.exists(logName))
  dir.create(logName)

for(i in 1:k)
{
  message("Writing csv log part ", i, " of ", k, " ", date())
  startRow <- (i-1)*1e6 + 1
  
  endRow <- ifelse(n-(i-1)*1e6 > 1e6, i*1e6, n)
  
  samplogPart <- samplog[startRow:endRow,]
  
  write.table(samplogPart, file.path(logPath, paste0(logName,"/",logName,"_", i, ".csv")), quote = F, sep=",", row.names = F, col.names = F)
}

samplogPart <- NULL
samplog <- NULL

message("Writing CSV logs to HDFS ", date())
system(intern = T, paste0("hdfs dfs -mkdir hdfs:///user/hive/warehouse/clustergps/", logName))
system(intern = T, paste0("hdfs dfs -put ", file.path(logPath, logName), " hdfs:///user/hive/warehouse/clustergps/"))

message("Creating Hive table")
system(intern = T, paste0("hive -e \"create external table clustergps.",logName,"(id string, time1 timestamp, latitude double, longitude double, hr int) row format delimited fields terminated by ',' lines terminated by '\n' location 'hdfs:///user/hive/warehouse/clustergps/",logName,"/';\""))

message("Starting clustering ", date())

source("sparkr-clustergps.R")