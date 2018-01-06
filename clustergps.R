library(ndjson)
library(lubridate)
library(sparklyr)
library(dplyr)
#library(compiler)

#enableJIT(level = 3)

#Sys.setenv('JAVA_HOME'='/usr/lib/jdk1.8.0_152/')

#spark_install(version = "2.1.0")

sc <- spark_connect(master = "local")

timeRanges <- c("7-17", "17-5")

message("Reading log")

samplog <- stream_in("part6.log")

message("Writing CSV")
#write out as csv
#samplogcsv <- samplog

samplog$timestamp <- as.POSIXct(samplog$timestamp/1000,origin='1970-01-01')
samplog$hr <- hour(samplog$timestamp)
#write.csv(samplogcsv, "samplog.csv", row.names = F)
#write.table(samplog, "samplog.csv", sep=",", quote=F, row.names=F, col.names=F)

#samplogcsv <- NULL

message("Copying to cluster")
#copy to spark cluster

samplog <- select(samplog, id, hr, timestamp, latitude, longitude)

samplog <- samplog %>% filter(id == 'fffd16d5-83f1-4ea1-95de-34b1fcad392b' |
                                id == 'fffc7412-deb1-4587-9c22-29ca833865ed' |
                                id == 'fffc68e3-866e-4be5-b1bc-5d21b89622ae')

samplog1 <- sdf_copy_to(sc, samplog, overwrite = T)

kms <- function(idLogs, group1, group2){
  
  km <- sparklyr::ml_kmeans(idLogs, centers = 3, features = c("latitude","longitude"))
  
  km1 <- copy_to(sc, km$centers, overwrite = T)
  
  cluster <-  sdf_predict(km)
  
  clustCounts <- cluster %>% group_by(prediction) %>% 
    tally  %>%
    mutate(conf=n/sum(n),
           prediction=prediction+1)
  
  clustCounts1 <- merge(clustCounts, km1, by.x=3, by.y=0)
  
  clustCounts1 <- copy_to(sc, clustCounts1, overwrite = T)
  
  clustCounts2 <- clustCounts1 %>% filter(., conf==max(conf)) %>% select(latitude, longitude, conf)
  
  return(clustCounts2)
}

samplog1 <- ft_vector_assembler(x = samplog1, input.col = c("latitude", "longitude"), output.col = "features")
sparklyr::ml_kmeans(x = samplog1, centers = 3,features = "features")
