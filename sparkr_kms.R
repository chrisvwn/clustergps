kms <- function(key, idLogs){
  tryCatch(
    {
      id <- as.character(key[1])
      
      day <- as.character(key[2])
      
      hr <- as.integer(key[3])
      
      if(length(unique(idLogs$latitude, idLogs$longitude)) < 3)
      {
        clustCounts <- data.frame(id=id, 
                                  day=day,
                                  hr=hr, 
                                  latitude=mean(idLogs$latitude), 
                                  longitude=mean(idLogs$longitude),
                                  conf=1/nrow(idLogs),
                                  stringsAsFactors = F)
      }
      else
      {
        idLogs <- dplyr::select(idLogs, latitude, longitude)
        
        km <- stats::kmeans(idLogs, centers = 3)
        
        centers <- km$centers
        
        centers <- cbind(centers, centerid=as.numeric(rownames(centers)))
        
        cluster <-  cbind(idLogs, prediction=km$cluster)
        
        clustCounts <- dplyr::mutate(dplyr::tally(dplyr::group_by(cluster, prediction)), conf=n/sum(n))
        
        clustCounts <- base::merge(clustCounts, centers, by.x="prediction", by.y="centerid")
        
        clustCounts <- dplyr::select(dplyr::filter(clustCounts, conf==max(conf)), latitude, longitude, conf)
        
        clustCounts <- data.frame(id, day, hr, clustCounts, stringsAsFactors = F)
      }
      
      #for(i in 1:ncol(clustCounts)) clustCounts[[i]] <- as.character(clustCounts[[i]])
      
      return(clustCounts)
    }, error = function(e) {
      return(
        data.frame(id=id, day=day, hr=hr, latitude=NA, longitude=NA, conf = NA, stringsAsFactors = F)
      )
    })
}