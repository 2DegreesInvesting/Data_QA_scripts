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


#### Connecting to Database------ run all together

  
  connect_factset_db <-
    function(dbname = "delta") {
      
      host <- "data-eval-db.postgres.database.azure.com"
      port <- 5432L
      
      username <- Sys.getenv("R_DATABASE_USER")
      password <- Sys.getenv("R_DATABASE_PASSWORD")
      
      if (username == "" | password == "") {
        # if username and password not defined in .env, look in systems keyring
        username <- "postgres@data-eval-db"
        
        if (rlang::is_installed("keyring")) {
          keyring_service_name <- "rmi_factset_database"
          
          if (!username %in% keyring::key_list(service = keyring_service_name)$username) {
            keyring::key_set(
              service = keyring_service_name,
              username = username,
              prompt = "Enter password for the FactSet database (it will be stored in your system's keyring): "
            )
          }
          password <- keyring::key_get(service = keyring_service_name, username = username)
        } else if (interactive() & rlang::is_installed("rstudioapi")) {
          password <- rstudioapi::askForPassword(
            prompt = "Please enter the FactSet database password:"
          )
        } else {
          cli::cli_abort(
            "No FactSet database password could be found. Please set the password
          as an environment variable"
          )
        }
      }
      
      conn <-
        DBI::dbConnect(
          drv = RPostgres::Postgres(),
          dbname = dbname,
          host = host,
          port = port,
          user = username,
          password = password,
          options = "-c search_path=fds"
        )
      
      reg_conn_finalizer(conn, DBI::dbDisconnect, parent.frame())
    }
  
  # connection finalizer to ensure connection is closed --------------------------
  # adapted from: https://shrektan.com/post/2019/07/26/create-a-database-connection-that-can-be-disconnected-automatically/
  
  reg_conn_finalizer <- function(conn, close_fun, envir) {
    
    is_parent_global <- identical(.GlobalEnv, envir)
    
    if (isTRUE(is_parent_global)) {
      env_finalizer <- new.env(parent = emptyenv())
      env_finalizer$conn <- conn
      attr(conn, 'env_finalizer') <- env_finalizer
      
      reg.finalizer(env_finalizer, function(e) {
        if (DBI::dbIsValid(e$conn)) {
          cat("Warning: A database connection was closed automatically because the connection object was removed or the R session was closed.")
          try(close_fun(e$conn))
        }
      }, onexit = TRUE)
      
    } else {
      withr::defer({
        if (DBI::dbIsValid(conn)) {
          dbname <- DBI::dbGetInfo(conn)$dbname
          host <- DBI::dbGetInfo(conn)$host
          
          cli::cli_warn(
            "The database connection to {.field {dbname}} on {.url {host}} was
        closed automatically because the calling environment was closed.",
            use_cli_format = TRUE
          )
          try(close_fun(conn))
        }
      }, envir = envir, priority = "last")
    }
    
    conn
  }
  
 ############### Alternative connection function. Don't Run if the above works

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

#key_delete("rmi_factset_database","postgres@data-eval-db", keyring = NULL)

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





