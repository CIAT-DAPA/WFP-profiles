# How it works
# query https://esgf-data.dkrz.de/esg-search/search/?offset=0&limit=100&type=Dataset&replica=false&latest=true&activity_id=CMIP&variable_id=pr%2Cta%2Ctasmax%2Ctasmin&mip_era=CMIP6&institution_id=BCC&frequency=day&experiment_id=historical&activity_id%21=input4MIPs&source_id=BCC-CSM2-MR&facets=mip_era%2Cactivity_id%2Cmodel_cohort%2Cproduct%2Csource_id%2Cinstitution_id%2Csource_type%2Cnominal_resolution%2Cexperiment_id%2Csub_experiment_id%2Cvariant_label%2Cgrid_label%2Ctable_id%2Cfrequency%2Crealm%2Cvariable_id%2Ccf_standard_name%2Cdata_node&format=application%2Fsolr%2Bjson
# pagination or increase the limit to 500 or more
# from the JSON output, capture xlink second item starting with http://hdl.handle.net/ 
# go to the http://hdl.handle.net/... page and look for http://...*.nc$ files
# most likely multiple download options are available ---> create a database of all relevant information
# all files https://esgf3.dkrz.de/thredds/catalog/esgcet/catalog.html

# Functions
# First search all results with few parameters
# complete list of search parameters are following
# TODO: function to provide more details/ validity of each parameter

library(xml2)
library(httr)
library(tidyverse)

findMetaCMIP6 <- function(...){
  
  # check validity of args supplied
  dots <- list(...)
  
  args <- names(dots)
  # args <- c("project", "activity_id", names(dots))
  
  # work in progress: not the final list 
  argsList <- c("offset","limit","activity_id","model_cohort","product","source_id",
                "institution_id","source_type","nominal_resolution","experiment_id",
                "sub_experiment_id","variant_label","grid_label","table_id","frequency",
                "realm","variable_id","cf_standard_name","data_node","project", "mip_era")
  
  # stop if non-matching argument supplied; pmatch/match.call is a better alternative to avoid errors
  nomatch <- args[!(args %in% argsList)]
  if(length(nomatch) > 0){stop(paste(paste(nomatch, collapse=","), " is not a recognized query parameter"))}
  
  # query structure to create GET request
  query = list(
    type = "Dataset",
    replica = "false",
    latest = "true",
    facets = "mip_era", # which cmip_era
    format = "application/solr+json"
  )
  
  # construct the query
  query <- c(dots, query)
  
  # evaluate query 
  # query <- eval(query)
  
  # llnl node no longer works, updated to dkrz
  # baseurl <- "https://esgf-node.llnl.gov/esg-search/search/"
  baseurl <- "https://esgf-data.dkrz.de/esg-search/search/"
  
  # get request
  request <- httr::GET(url = baseurl, query = query)
  httr::stop_for_status(request)
  
  response <- httr::content(request, as = "text")
  dj <- jsonlite::fromJSON(response)
  return(dj$response$docs)
}

getURLs <- function(d) {
  uud <- list()
  pb <- txtProgressBar(max=nrow(d), style = 3)
  for (i in 1:nrow(d)) {
    ds <- d[i,]
    # read XML metadata
    u <- unlist(ds$url)[1]
    stopifnot (length(u) > 0)
    u <- try(xml2::read_html(u), silent = TRUE) 
    # if xml content is missing
    if (any(class(u) == "try-error")) next
    # get dataset nodes
    nds <- xml2::xml_find_all(u, ".//dataset")
    # find the ones with files
    uu <- lapply(nds, getAccess)
    uu[sapply(uu, is.null)] <- NULL
    uu <- data.table::rbindlist(uu, fill = TRUE)
    # construct url for data access
    uu$file_url <- file.path("http:/",ds$data_node, "thredds/fileServer", uu$file_url)
    uud[[i]] <- data.frame(ds[rep(seq_len(nrow(ds)), each = nrow(uu)), ], uu)
    setTxtProgressBar(pb, i) 
  }
  close(pb)
  return(uud)
}	


getMetaCMIP6 <- function(...) {

  # cat("Parsing page ", i, "-----------------\n"); flush.console()
  # 10000 is the maximum limit in one query
  xdd <- try(findMetaCMIP6(...))

  ldd <- getURLs(xdd)
  sdd <- data.table::rbindlist(ldd, fill = TRUE)
  names(sdd)[names(sdd) == 'size.1'] <- 'file_size'
  # are there any repeatation
  sdd <- unique(sdd, by = "file_id")
  ftime <- sapply(strsplit(sdd$file_name, "_"), function(x)grep(".nc", x, value = TRUE))
  ftime <- strsplit(gsub(".nc", "", ftime), "-")
  stime <- sapply(ftime, "[[", 1)
  etime <- sapply(ftime, "[[", 2)
  sdd$file_start_date <- as.Date(stime, "%Y%m%d")
  sdd$file_end_date <- as.Date(etime, "%Y%m%d")
  tokeep <- c("id","version", "activity_id","cf_standard_name","variable","variable_units","data_node","experiment_id", 
              "frequency","index_node","institution_id","member_id","mip_era","file_name", "file_url",
              "nominal_resolution","file_start_date","file_end_date","number_of_aggregations",
              "number_of_files","pid","project","source_id","source_type","sub_experiment_id", 
              "url","xlink","realm","replica","latest","geo","geo_units","grid","mod_time", "file_size" ,"checksum","checksum_type",
              "grid_label","data_specs_version","tracking_id","citation_url", "further_info_url", "retracted")
  sdd <- sdd[, ..tokeep]
  return(sdd)
}

# complete list of files
# additional cases
# d <- getMetaCMIP6(nominal_resolution = "100km"))
# may be not honored in the search
# d <- try(findMetaCMIP6(offset = offset, limit = limit, experiment_id = "historical"))



# helper function to find matches in list
getAccess <- function(nd, pattern = ".nc$"){
  urlp <- xml2::xml_attr(nd, "urlpath")
  if(!is.na(urlp)){
    dataset <- xml2::xml_attr(nd, "name")
    if (grepl(pattern, dataset)){
      xd <- xml_contents(nd)
      xdd <- bind_rows(lapply(xml_attrs(xd), function(x) data.frame(as.list(x), stringsAsFactors=FALSE)))
      xdd <- xdd[,c("name", "value")]
      xdd <- xdd[complete.cases(xdd),]
      d <- data.frame(t(xdd$value))
      colnames(d) <- xdd$name
      d <- data.frame(file_name = dataset, file_url = urlp, d)
      return(d)}
  }
}

# get important parameters from uu: urlPath and dataset
getDataCMIP6 <- function(d, downdir, silent=FALSE){
  if (!silent) print(d$furl); flush.console()
  # specify where to save
  flocal <- file.path(downdir, d$source_id, basename(d$furl)) #uu[1])
  if (file.exists(flocal)) return()
  dir.create(dirname(flocal), FALSE, TRUE)
  # start downloading
  download.file(d$furl, flocal, mode = "wb", quiet=silent)
  # alternate
  # curl::curl_fetch_disk(fileurl, file.path(localdir, dataset))
}



# instead of own search, rely on others 
# library(data.table)
# url <- "https://storage.googleapis.com/cmip6/pangeo-cmip6.csv"
# cmip6cat <- paste0("data/", basename(url))
# download.file(url, destfile = cmip6cat, mode = "wb")
# adf <- fread(cmip6cat)