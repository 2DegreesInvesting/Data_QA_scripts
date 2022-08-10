##### loading libraries
library(here)
library(fst)
library(odbc)
library(RPostgres)
library(dplyr)
library(dbplyr)
library(readr)
library(readxl)
library(keyring)


#### Connecting to Database

connect_factset_db <-
  function(dbname = "delta") {
    require(keyring)
    require(odbc)
    require(RPostgres)
    
    host <- "data-eval-db.postgres.database.azure.com"
    port <- 5432L
    
    keyring_service_name <- "rmi_factset_db"
    username <- "postgres@data-eval-db"
    
    if (!username %in% key_list(service = keyring_service_name)$username) {
      key_set(
        service = keyring_service_name,
        username = username,
        prompt = "Enter password for the FactSet database (it will be stored in your system's keyring): "
      )
    }
    password <- key_get(service = keyring_service_name, username = username)
    
    odbc::dbConnect(
      drv = RPostgres::Postgres(),
      dbname = dbname,
      host = host,
      port = port,
      user = username,
      password = password,
      sslmode = "require",
      options = "-c search_path=fds"
    )
  }

### if you want to delete your keyring from your computer
#key_list()

#key_delete("rmi_factset_db","postgres@data-eval-db", keyring = NULL)

factset_db <- connect_factset_db()

#### calling database tables

iss_climate_core <- tbl(factset_db, "icc_v2_icc_carbon_climate_core") %>%
  collect()

iss_factset_id_map <- tbl(factset_db, "icc_v2_icc_factset_id_map") %>%
  collect() 

iss_provider_id_map <- tbl(factset_db, "icc_v2_icc_provider_id_map") %>%
  collect() 

iss_sec_entity_hist <- tbl(factset_db, "icc_v2_icc_sec_entity_hist") %>%
  collect() 


#### qa data tables





dbDisconnect(factset_db)





