# library(sparklyr)
# sc <- spark_connect(master='local', packages=TRUE)
# iris2 <- iris[,1:(ncol(iris) - 1)]
# df1 <- sdf_copy_to(sc, iris2, repartition=5, overwrite=T)
# 
# # This works fine
# res <- spark_apply(df1, function(x) kmeans(x, 3)$centers)
# 
# #################################
spark_apply <- function(x,
                        f,
                        columns = colnames(x),
                        memory = TRUE,
                        group_by = NULL,
                        packages = TRUE,
                        ...) {
  args <- list(...)
  assertthat::assert_that(is.function(f))
  
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)
  sdf_columns <- colnames(x)
  rdd_base <- invoke(sdf, "rdd")
  grouped <- !is.null(group_by)
  args <- list(...)
  rlang <- spark_config_value(sc, "sparklyr.closures.rlang", FALSE)
  proc_env <- connection_config(sc, "sparklyr.apply.env.")
  
  # backward compatible support for names argument from 0.6
  if (!is.null(args$names)) {
    columns <- args$names
  }
  
  columns_typed <- length(names(columns)) > 0
  
  if (rlang) warning("The `rlang` parameter is under active development.")
  
  # disable package distribution for local connections
  if (spark_master_is_local(sc$master)) packages <- FALSE
  
  # create closure for the given function
  closure <- serialize(f, NULL)
  
  # create rlang closure
  rlang_serialize <- spark_apply_rlang_serialize()
  closure_rlang <- if (rlang && !is.null(rlang_serialize)) rlang_serialize(f) else raw()
  
  # create a configuration string to initialize each worker
  worker_config <- worker_config_serialize(
    c(
      list(
        debug = isTRUE(args$debug)
      ),
      sc$config
    )
  )
  
  if (grouped) {
    colpos <- which(colnames(x) %in% group_by)
    if (length(colpos) != length(group_by)) stop("Not all group_by columns found.")
    
    group_by_list <- as.list(as.integer(colpos - 1))
    
    grouped_rdd <- invoke_static(sc, "sparklyr.ApplyUtils", "groupBy", rdd_base, group_by_list)
    
    rdd_base <- grouped_rdd
    
    if (!columns_typed) {
      columns <- c(group_by, columns)
    }
  }
  
  worker_port <- spark_config_value(sc$config, "sparklyr.gateway.port", "8880")
  
  bundle_path <- ""
  if (packages) {
    bundle_path <- core_spark_apply_bundle_path()
    if (!file.exists(bundle_path)) {
      bundle_path <- core_spark_apply_bundle()
    }
    
    if (!is.null(bundle_path)) {
      bundle_was_added <- file.exists(
        invoke_static(
          sc,
          "org.apache.spark.SparkFiles",
          "get",
          basename(bundle_path))
      )
      
      if (!bundle_was_added) {
        spark_context(sc) %>% invoke("addFile", bundle_path)
      }
    }
  }
  
  rdd <- invoke_static(
    sc,
    "sparklyr.WorkerHelper",
    "computeRdd",
    rdd_base,
    closure,
    worker_config,
    as.integer(worker_port),
    as.list(sdf_columns),
    as.list(group_by),
    closure_rlang,
    bundle_path,
    as.environment(proc_env),
    as.integer(60)
  )
  
  # while workers need to relaunch sparklyr backends, cache by default
  if (memory) rdd <- invoke(rdd, "cache")
  
  schema <- spark_schema_from_rdd(sc, rdd, columns)
  
  transformed <- invoke(hive_context(sc), "createDataFrame", rdd, schema)
  
  sdf_register(transformed)
}