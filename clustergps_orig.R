library(ndjson)
library(ggplot2)
library(lubridate)

timeRanges <- c("8-17", "18-22")

message("Reading log")

samplog <- stream_in("part6.log")

samplog$timestamp <- as.POSIXct(samplog$timestamp/1000,origin='1970-01-01')

userIds <- unique(samplog$id)

likelyLocs <- NULL

for(userId in userIds)
{
  message("userid:", userId, ":", which(userIds == userId))
  
  idLogDays <- unique(as.Date(samplog$timestamp))
  
  likelyLoc <- NULL
  
  for(idx in seq_along(idLogDays))
  {
    idLogDay <- idLogDays[idx]
    
    message("day:", idLogDay)
    
    idLogDayData <- subset(samplog, id == userId & as.Date(timestamp) == idLogDay)
    
    for(timeRange in timeRanges)
    {
      message("timeRange:", timeRange)
      
      times <- unlist(strsplit(timeRange, "-"))
      timeFrom <- as.numeric(times[1])
      timeTo <- as.numeric(times[2])
      
      idLogs <- idLogDayData[which(hour(idLogDayData$timestamp) >= timeFrom & 
                                     hour(idLogDayData$timestamp) <= timeTo),]
      
      idLogLocs <- cbind(idLogs$latitude, idLogs$longitude)
      
      idLogLocs <- unique(idLogLocs)
      
      n <- nrow(idLogLocs)
      
      if(n > 0)
      {
        if(n > 4)
          km <- kmeans(idLogLocs, centers = 4)
        else
          km <- kmeans(idLogLocs, centers = 1)
        
        idLogs$cluster <- km$cluster
        
        clustCounts <- table(idLogs$cluster)
        
        highestClust <- which(clustCounts == max(clustCounts))[1]
        
        maxCountLat <- idLogs[which(cluster == highestClust)[1], latitude]
        maxCountLon <- idLogs[which(cluster == highestClust)[1], longitude]
        
        clustConf <- max(clustCounts) / sum(clustCounts)
      }
      else
      {
        maxCountLat = NA
        maxCountLon = NA
        clustConf = NA
      }
      
      if(is.null(likelyLoc))
        likelyLoc <- cbind.data.frame(day=idLogDay, timeRange, lat=as.numeric(maxCountLat), lon=as.numeric(maxCountLon), conf=as.numeric(clustConf))
      else
        likelyLoc <- cbind.data.frame(likelyLoc, day=idLogDay, timeRange, lat=as.numeric(maxCountLat), lon=as.numeric(maxCountLon), conf=as.numeric(clustConf))
      
    }
    
    if(is.null(likelyLoc))
      likelyLoc <- cbind.data.frame(day=idLogDay, timeRange, lat=as.numeric(maxCountLat), lon=as.numeric(maxCountLon), conf=as.numeric(clustConf))
  }
  
  likelyLocs <- rbind.data.frame(likelyLocs, cbind.data.frame(id=userId, likelyLoc))
}

write.csv(likelyLocs, "likelylocs.csv")
