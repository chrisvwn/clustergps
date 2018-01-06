kms <- function(idLogs){
  
  km <- sparklyr::ml_kmeans(idLogs, centers = 3, features = c("latitude","longitude"))
  
  km1 <- copy_to(sc, km$centers, overwrite = T)
  
  return(km1[1,])
  
  cluster <-  sdf_predict(km)
  
  clustCounts <- cluster %>% group_by(prediction) %>% 
    tally  %>%
    mutate(conf=n/sum(n),
           prediction=prediction+1)
  
  clustCounts <- merge(clustCounts, km1, by.x=3, by.y=0)
  
  #clustCounts <- copy_to(sc, clustCounts, overwrite = T)
  
  clustCounts <- clustCounts %>% filter(., conf==max(conf)) %>% select(latitude, longitude, conf)
  
  return(clustCounts)
}