source("kms.R")
source("kms2.R")

timeRanges <- c("7-17", "17-5")

message("Connecting to spark ", date())
sparkEnvir <- list(#spark.spark.num.executors='1',
  #spark.driver.memory = "2g",
  #spark.executor.cores='2',
  #spark.executor.memory='1g',
  spark.dynamicAllocation.enabled = "true",
  spark.dynamicAllocation.initialExecutors="40",
  spark.rdd.compress = "true",
  spark.shuffle.service.enabled = "true",
  appName = "RStudioSparkR",
  spark.sql.broadcastTimeout = "3600"
)

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

Sys.setenv("SPARKR_SUBMIT_ARGS"="--master yarn sparkr-shell")

sc <- sparkR.session(master = "yarn", sparkConfig = sparkEnvir)

#setLogLevel("DEBUG")

#samplog <- sql("SELECT * FROM clustergps.part4 WHERE id in (select distinct(id) from clustergps.part6 limit 10)")

message("Reading in log from Hive table ", date())
samplog <- sql(paste0("SELECT * FROM clustergps.", logName, " LIMIT 10"))

message("Converting date to string ", date())
samplog <- SparkR::mutate(samplog, day=date_format(samplog$time1, 'yyyy-MM-dd'))


message("Planning hourly kmeans calculation ", date())
schema <- structType(structField("id", "string"),
                     structField("day", "string"),
                     structField("hr", "integer"),
                     structField("latitude", "double"),
                     structField("longitude", "double"),
                     structField("confidence", "double"))

result <- gapply(
  samplog,
  c("id", "day", "hr"),
  kms,
  schema)

message("Planning append of grouping column by timeRanges ", date())

#build CASE expression to be used in selectExpr
grpStmt <- paste(lapply(1:length(timeRanges),
                        function(i){ 
                          x <- unlist(strsplit(timeRanges[i], "-")); 
                          paste0("WHEN hr >= ", x[1], 
                                 ifelse(as.numeric(x[1]) < as.numeric(x[2])," AND ", " OR "),
                                 "hr <= ", x[2], " THEN '", 
                                 timeRanges[i], "'") 
                        }),
                 collapse = " ")

grpStmt <- paste0("CASE ", grpStmt, " ELSE 'NA' END AS gp")

#Create new col assigning groups based on timeRanges
result <- merge(result, selectExpr(result, grpStmt))

#remove any rows that are not within the time ranges
result <- filter(result, result$gp != 'NA')

message("Executing and saving tempkms24hr to HDFS ", date())
SparkR::write.df(df = result,
                 path =paste0("/user/hive/warehouse/clustergps/",logName,"_tempkms24hr"),
                 source="parquet",
                 mode="overwrite")

message("Planning calculation of id, day kmeans from hourly kmeans ", date())
#get the kmeans of the hours in the 24 hour period that fit in the time ranges

temp1 <- read.df(paste0("/user/hive/warehouse/clustergps/",logName,"_tempkms24hr"))

#group by id, day and gp
resGpd <- groupBy(temp1, "id", "day", "gp")

schema1 <- structType(structField("id", "string"),
                      structField("day", "string"),
                      structField("gp", "string"),
                      structField("latitude", "double"),
                      structField("longitude", "double"),
                      structField("confidence", "double"))

#run kmeans again
result1 <- gapply(
  resGpd,
  kms2,
  schema1)

#message("Executing and saving temp2kmstimeRanges to HDFS ", date())
#SparkR::write.df(df = result1,
#                 path =paste0("/user/hive/warehouse/clustergps/",logName,"_temp2kmstimeRanges"),
#                 source="parquet",
#                 mode="overwrite")

message("Planning output format of likelyLocs ", date())

#temp2 <- read.df(paste0("/user/hive/warehouse/clustergps/",logName,"_temp2kmstimeRanges"))

#reformat the output so each row has id, day and likelylocs per timeRange
resGpdById <- groupBy(result1, "id", "day")

schema2 <- structType(structField("id", "string"),
                      structField("day", "string"),
                      structField("likelyLoc", "string"))


locRows <- function(key, idLogs){
  tryCatch(
    {
      id <- as.character(key[1])
      
      day <- as.character(key[2])
      
      #sort by day and hour
      idLogs <- idLogs[order(idLogs$day, idLogs$gp),]
      
      #paste id, day, timeRangeIdx|latitude|longitude|conf
      likelyLoc <- data.frame(id=id, day=day, locs=paste(apply(idLogs[,-c(1,2)], 1, paste, collapse="|"), collapse=","), stringsAsFactors = F)
      
      return(likelyLoc)
    }, error = function(e) {
      return(
        data.frame(id=id, day=day, locs="NA", stringsAsFactors = F)
      )
    })
}

likelyLocs <- gapply(
  resGpdById,
  locRows,
  schema2)

#likelyLocsDF <- collect(likelyLocs)

#message("Replacing timeRange index with string ", date())
#
#replace timeRange index with timeRange string
# for(i in 1:length(timeRanges))
# {
#   likelyLocs <- withColumn(likelyLocs, 
#                            "likelyLoc", 
#                            regexp_replace(likelyLocs$likelyLoc, 
#                                           paste0("[^|,](",i,"\\\\|)(.*)"), 
#                                           paste0(timeRanges[i],"$2")))
# }

message("Formatting final output and saving to HDFS ", date())

#save output to hdfs
SparkR::write.df(df = likelyLocs,
                 path =paste0("/user/hive/warehouse/clustergps/",logName,"_likelylocs"),
                 source="parquet",
                 mode="overwrite")

message("Execution DONE ", date())
message("If no errors occurred output is in ",paste0("/user/hive/warehouse/clustergps/",
                                                     logName,"_likelylocs"))

#read in likelylocs results if small enough
#res <- collect(read.parquet("/hive/warehouse/clustergps/part4_likelylocs"))

#write.csv(likelyLocsDF, file.path(logPath,paste0(logName,"_likelylocs.csv")), row.names = F)
