source("kms.R")

timeRanges <- c("7-17", "17-5")

sparkEnvir <- list(#spark.spark.num.executors='1',
  #spark.driver.memory = "2g",
  #spark.executor.cores='2',
  #spark.executor.memory='1g',
  spark.dynamicAllocation.enabled = "true",
  spark.dynamicAllocation.initialExecutors="40",
  spark.rdd.compress = "true",
  spark.shuffle.service.enabled = "true",
  appName = "RStudioSparkR"
)

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

Sys.setenv("SPARKR_SUBMIT_ARGS"="--master yarn sparkr-shell")

sc <- sparkR.session(master = "yarn", sparkConfig = sparkEnvir)

#samplog <- sql("SELECT * FROM clustergps.part4 WHERE id in (select distinct(id) from clustergps.part6 limit 10)")

samplog <- sql("SELECT * FROM clustergps.part4 limit 10000")

samplog <- SparkR::mutate(samplog, day=date_format(samplog$time1, 'yyyy-MM-dd'))

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

res <- collect(result)

SparkR::write.df(df = result,
                 path ="/user/hive/warehouse/clustergps/part4_result",
                 source="parquet",
                 mode="overwrite")

res <- collect(read.parquet("/hive/warehouse/clustergps/part4_result"))

grpStmt <- lapply(1:length(timeRanges), 
                  function(i){ 
                    x <- unlist(strsplit(timeRanges[i], "-")); 
                    paste0("(y >= ", x[1], 
                           ifelse(as.numeric(x[1]) < as.numeric(x[2])," & ", " | "),
                           "y <= ", x[2], ") ", 
                           i) 
                  })

grpStmt <- paste("if ", grpStmt, collapse = " else ")
grpStmt <- paste0(grpStmt, paste0(" else ", length(timeRanges)+1))

res$gp <- sapply(res$hr, function(y) eval(expr=parse(text = grpStmt)))

res <- as.DataFrame(res)

resGpd <- groupBy(res, "id", "day", "gp")

schema1 <- structType(structField("id", "string"),
                      structField("day", "string"),
                      structField("hr", "integer"),
                      structField("latitude", "double"),
                      structField("longitude", "double"),
                      structField("confidence", "double"))

result1 <- gapply(
  resGpd,
  kms,
  schema1)


resGpdById <- groupBy(result1, "id", "day")

schema2 <- structType(structField("id", "string"),
                      structField("day", "string"),
                      structField("likelyLoc", "string"))


locRows <- function(key, idLogs){
  tryCatch(
    {
      id <- as.character(key[1])
      
      day <- as.character(key[2])
      
      idLogs <- idLogs[order(idLogs$day, idLogs$hr),]
      
      likelyLoc <- data.frame(id=id, day=day, locs=paste(apply(idLogs[,-1], 1, paste, collapse="|"), collapse=","), stringsAsFactors = F)
      
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

likelyLocsDF <- collect(likelyLocs)

for(i in 1:length(timeRanges))
  likelyLocsDF$likelyLoc <- stringr::str_replace(likelyLocsDF$likelyLoc, paste0("\\|",i,"\\|"), paste0("\\|", timeRanges[i],"\\|"))

write.csv(likelyLocsDF, "likelylocsdf.csv", row.names = F)
