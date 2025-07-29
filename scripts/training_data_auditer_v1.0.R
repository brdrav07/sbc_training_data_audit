################################################################################################################################################################
#            ███████╗██████╗  ██████╗    ████████╗██████╗  █████╗ ██╗███╗   ██╗██╗███╗   ██╗ ██████╗     ██████╗  █████╗ ████████╗ █████╗                      #
#            ██╔════╝██╔══██╗██╔════╝    ╚══██╔══╝██╔══██╗██╔══██╗██║████╗  ██║██║████╗  ██║██╔════╝     ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗                     #
#            ███████╗██████╔╝██║            ██║   ██████╔╝███████║██║██╔██╗ ██║██║██╔██╗ ██║██║  ███╗    ██║  ██║███████║   ██║   ███████║                     #
#            ╚════██║██╔══██╗██║            ██║   ██╔══██╗██╔══██║██║██║╚██╗██║██║██║╚██╗██║██║   ██║    ██║  ██║██╔══██║   ██║   ██╔══██║                     #
#            ███████║██████╔╝╚██████╗       ██║   ██║  ██║██║  ██║██║██║ ╚████║██║██║ ╚████║╚██████╔╝    ██████╔╝██║  ██║   ██║   ██║  ██║                     #
#            ╚══════╝╚═════╝  ╚═════╝       ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝╚═╝  ╚═══╝╚═╝╚═╝  ╚═══╝ ╚═════╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝                     #
################################################################################################################################################################                                                                                                                               

#$$$ Authored by Brodie Verrall
#$$$ Last updated: 2025-07-29

#$$$ This script pulls together the various sources of potential training data and assesses each site against a series of auditing criteria
#TODO: ensure all training data sources are current and stored in ...project_data/training_data_inputs


# This script is dependent on QBERD, QBEIS and RAPID site summaries and scoring databricks workflows. 
# Other misc training sources are stored locally and are not dynamic

#     ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗
#    ██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝
#   ██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝   
#  ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝    

### 0) Workspace setup ### -------------------------------------------------------------------------------------------------------------------------------------
# # 0.1) Check renv status and clean workspace (optional)
# renv::status()
# rm(list = ls())
# gc()

# 0.2) Load project library
library(tidyverse)
library(purrr)
library(furrr)
library(fs)
library(here)
library(jsonlite)
library(sf)
library(lwgeom)
library(terra)

# 0.3) clean workspace, set up directories and wd
proj_folders <- c("archive", "project_data", "scripts")
invisible(lapply(proj_folders, function(folder) {
  path <- here(folder)
  if (!dir.exists(path)) {
    dir.create(path)
    message(sprintf("'%s' folder created.", folder))
  } else {
    message(sprintf("'%s' folder already exists.", folder))
  }
}))
setwd(here("project_data"))
message("Working directory set to: ", getwd())

subfolders <- c("training_data_inputs", "spatial_inputs", "tabular_inputs", "intermediates", "outputs")
invisible(lapply(subfolders, function(folder) {
  if (!dir.exists(folder)) {
    dir.create(folder)
    message(sprintf("'%s' folder created.", folder))
  } else {
    message(sprintf("'%s' folder already exists.", folder))
  }
}))
int_dir <-file.path("intermediates/")
output_dir <- file.path("outputs/")

### TODO: need to assign RE and score sites in separate script, call it here and then pull in the output (SBC_TD_pool.csv)
# 0.4) Import training data sources
# qberd <- read.csv("training_data_inputs/QBERD_TEST.csv")
# qbeis <- read.csv("training_data_inputs/QBEIS_TEST.csv")
# rapid <- read.csv("training_data_inputs/RAPID_TEST.csv")
# tern <- read.csv("training_data_inputs/TERN_TEST.csv")
# bcc <- read.csv("training_data_inputs/BCC_TEST.csv")
# qval <- read.csv("training_data_inputs/QVAL_TEST.csv")
# quat <- read.csv("training_data_inputs/QUAT_TEST.csv")
# desktop <- read.csv("training_data_inputs/DESKTOP_TEST.csv")
# inferred <- read.csv("training_data_inputs/INFERRED_TEST.csv")
poi <- read.csv("training_data_inputs/REv13_POI_TEST.csv") # Use this large dummy POI dataset for testing

# 0.5) Import other required tabular data
redd <- read.csv("tabular_inputs/REDD_v13.1_2024.csv")
re <- read.csv("tabular_inputs/regional_ecosystem_2024.csv")

### TODO: Intergrate spatial data in each section to pipe in and kick out only when needed
# SBC_td_v7 <-  sf::st_read("spatial_inputs/SBC_TDv7.5_WOS_BVG_STRUCTURE.gpkg", quiet = TRUE) # last version of WOS td
# pre_clear <- sf::st_read("spatial_inputs/Preclear_v13-1_2021/data.gdb", quiet = TRUE) # pre-clearance RE mapping
# remnant <- sf::st_read("spatial_inputs/Remnant_v13-1_2021/data.gdb", quiet = TRUE) # remnant vegetation mapping
# hvr <- sf::st_read("spatial_inputs/HVR_v13-1-1_2021/data.gdb", quiet = TRUE) # high value regrowth mapping
# burn_scars <- terra::rast("spatial_inputs/Fire_scars_2025/IMG_QLD_SENTINEL2_ANNUAL_FIRESCARS_2020/cvmsre_qld_2020_afma2.tif") # burn scars
# slats_clearing <- sf::st_read("spatial_inputs/SLATS_2023/SLATS_9195/data.gdb", quiet = TRUE) #slats clearing 
# land_use <- sf::st_read("spatial_inputs/QLD_LANDUSE_June_2019/QLD_LANDUSE_June_2019.gdb", quiet = TRUE) # Queensland land use
# sentinel_grid <-


#     ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗
#    ██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝
#   ██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝   
#  ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝    

### 1) Tabular processing ### ==================================================================================================================================

# 1.1) Add structure code to complied TD -----------------------------------------------------------------------------------------------------------------------
poi <- poi %>%
  left_join(re %>% select (NAME, STRUCTURE_CODE),
            by = c("RE1" = "NAME")) %>%
  relocate(STRUCTURE_CODE, .after = DBVG1M)

# 1.2) Add fire guidelines to complied TD ----------------------------------------------------------------------------------------------------------------------
poi <- poi %>%
  mutate(RE1_modified = sub("^(\\d+\\.\\d+\\.\\d+).*", "\\1", RE1)) %>%
  left_join(redd %>% select(NAME, FIRE_GUIDELINES),
            by = c("RE1_modified" = "NAME")) %>%
  select(-RE1_modified) %>% 
  relocate(FIRE_GUIDELINES, .after = COLLECTION_NOTES)

# 1.3) Add GDA94, GDA2020 coordinates --------------------------------------------------------------------------------------------------------------------------
# Define a function that processes poi and returns it with new columns
add_gda_coords <- function(df, x_col = "x_3577", y_col = "y_3577") {
  # Convert to sf
  df_sf <- st_as_sf(df, coords = c(x_col, y_col), crs = 3577)
  
  # Extract GDA94 coords
  gda94 <- st_transform(df_sf, 4283) %>%
    mutate(
      x_gda94 = st_coordinates(.)[, 1],
      y_gda94 = st_coordinates(.)[, 2]
    ) %>%
    st_drop_geometry() %>%
    select(x_gda94, y_gda94)
  
  # Extract GDA2020 coords
  gda2020 <- st_transform(df_sf, 7844) %>%
    mutate(
      x_gda2020 = st_coordinates(.)[, 1],
      y_gda2020 = st_coordinates(.)[, 2]
    ) %>%
    st_drop_geometry() %>%
    select(x_gda2020, y_gda2020)
  
  # Bind new columns and relocate
  df <- df %>%
    bind_cols(gda94) %>%
    bind_cols(gda2020) %>%
    relocate(x_gda94, y_gda94, x_gda2020, y_gda2020, .after = ACCURACY)
  
  return(df)
}

# Apply the function (no intermediates left behind)
poi <- add_gda_coords(poi)

# Ensure poi is in the same CRS as the vector data
poi_sf <- st_as_sf(poi, coords = c("x_gda94", "y_gda94"), crs = 3577)  ### MAY HAVE TO SWAP TO 3577 

# Convert to terra vect for efficient extraction
poi_vect <- vect(poi_sf)

# Set chunk size (adjustable)
chunk_size <- 50000  
n_chunks <- ceiling(nrow(poi_vect) / chunk_size)

#     ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗ ██╗
#    ██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝
#   ██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝██╔╝   
#  ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝ ╚═╝    

### 2) Spatial processing ### ==================================================================================================================================

# 2.3) Burn scars ----------------------------------------------------------------------------------------------------------------------------------------------
# 2.3.1) Scrape and import burn scar rasters ...................................................................................................................

# Get list of all .tif files in Fire_scars folder
tif_files <- list.files(path = "spatial_inputs/burn_scars", pattern = "\\.tif$", full.names = TRUE, recursive = TRUE)

# 2.3.2) Sample the raster in chunks ...........................................................................................................................
# Process each raster file
for(tif_file in tif_files) {
  # Extract year from filename (assuming format like "firescar_2000.tif")
  year <- gsub(".*?(\\d{4}).*", "\\1", basename(tif_file))
  col_name <- paste0("FireMonth_", year)
  
  message("Processing: ", basename(tif_file), " (", col_name, ")")
  
  # Load raster
  r <- rast(tif_file)
  
  # Initialize result vector
  sampled_values <- rep(NA, nrow(poi_vect))
  
  # Process in chunks
  n_chunks <- ceiling(nrow(poi_vect) / chunk_size)
  
  for(i in seq_len(n_chunks)) {
    start_idx <- (i - 1) * chunk_size + 1
    end_idx <- min(i * chunk_size, nrow(poi_vect))
    
    chunk <- poi_vect[start_idx:end_idx]
    sampled_values[start_idx:end_idx] <- terra::extract(r, chunk)[, 2]
    
    rm(chunk)
    gc()
    
    message(sprintf("  Chunk %d/%d processed", i, n_chunks))
  }
  
  # Add results to poi_sf
  poi_sf[[col_name]] <- sampled_values
  
  # Clean up
  rm(r, sampled_values)
  gc()
}

rm(tif_file, tif_files)
gc()

# Save intermediate fire sampled sites
st.write(poi_sf, file.path(int_dir, "fire_poi.gpkg"), quiet = TRUE, overwrite = TRUE) 

# 2.3.3) Add burn scar flag ....................................................................................................................................



# Extract collection date and RE min fire return interval

# calculate number of burns since 1987

# calculate fire return interval

# Calculate burns after collection and most recent burn


################################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################

############### CAN IMPORT FIRE SAMPLED POI HERE TO SKIP PROCESSING ###############
poi_sf <- st_read("intermediates/fire_poi.gpkg", quiet = TRUE) # Load pre-sampled fire data
###################################################################################   

# Fire columns in ascending order by year
col_names <- colnames(poi_sf)
firemonth_cols <- grep("^FireMonth_", col_names, value = TRUE)
sorted_firemonth_cols <- firemonth_cols[order(as.numeric(sub("FireMonth_", "", firemonth_cols)))]
reordered_cols <- c(setdiff(col_names, firemonth_cols), sorted_firemonth_cols)
poi_sf <- poi_sf[, reordered_cols]

# Function to process fire metrics
process_fire_metrics <- function(poi_sf, start_year = 1987) {
  # Extract fire columns and ensure they are numeric
  fire_columns <- poi_sf %>%
    st_drop_geometry() %>%
    select(starts_with("FireMonth_")) %>%
    mutate(across(everything(), as.numeric))
  
  # Convert fire columns to a numeric matrix
  fire_months_matrix <- as.matrix(fire_columns)
  
  # Extract fire years from column names
  fire_years <- as.numeric(str_extract(names(fire_columns), "\\d{4}"))
  
  # Filter valid fire months (1–12 only)
  valid_fire_mask <- fire_months_matrix >= 1 & fire_months_matrix <= 12
  valid_fire_months <- ifelse(valid_fire_mask, fire_months_matrix, NA)
  
  # Find the most recent burn
  most_recent_burn <- apply(valid_fire_months, 1, function(row) {
    # Find the first non-NA value from the right (most recent column)
    for (i in length(row):1) {
      if (!is.na(row[i])) {
        year <- fire_years[i]
        month <- row[i]
        return(as.Date(sprintf("%04d-%02d-01", year, month)))  # Return as Date object
      }
    }
    return(NA)  # Return NA if no valid burns are found
  })
  
  # Explicitly convert the result to a Date vector
  most_recent_burn <- as.Date(most_recent_burn, origin = "1970-01-01")
  
  # Calculate metrics
  fire_indices <- (fire_years - start_year) * 12 + valid_fire_months
  fire_intervals <- apply(fire_indices, 1, function(x) {
    valid_indices <- na.omit(x)
    if (length(valid_indices) > 1) diff(sort(valid_indices)) else NA
  })
  
  fire_return_interval <- sapply(fire_intervals, function(x) {
    if (is.null(x) || all(is.na(x))) {
      NA  # Return NA if x is NULL or all values are NA
    } else {
      mean(x, na.rm = TRUE)  # Calculate the mean if x has valid values
    }
  })
  
  collection_date <- dmy(poi_sf$COLLECTION_DATE)
  collection_index <- (year(collection_date) - start_year) * 12 + month(collection_date)
  
  burns_after_collection <- rowSums(fire_indices > collection_index, na.rm = TRUE)
  
  # Add metrics to poi_sf
  poi_sf <- poi_sf %>%
    mutate(
      num_burns = rowSums(!is.na(valid_fire_months), na.rm = TRUE),
      fire_intervals = fire_intervals,
      fire_return_interval = fire_return_interval,
      collection_date = collection_date,
      collection_index = collection_index,
      burns_after_collection = burns_after_collection,
      most_recent_burn = most_recent_burn,  # Already a Date object
      recommended_interval = as.numeric(str_extract(FIRE_GUIDELINES, "(?<=INTERVAL_MIN: )\\d+"))
    )
  
  return(poi_sf)
}

# Function to calculate FD_FLAG and 3-year windows
calculate_fd_flags <- function(poi_sf, fixed_dates) {
  collection_date <- dmy(poi_sf$COLLECTION_DATE)
  time_since_fire <- ifelse(
    !is.na(poi_sf$most_recent_burn),
    (year(collection_date) - year(poi_sf$most_recent_burn)) * 12 + 
      (month(collection_date) - month(poi_sf$most_recent_burn)),
    NA
  )
  
  fire_after_collection <- !is.na(poi_sf$most_recent_burn) & poi_sf$most_recent_burn > collection_date
  
  time_to_fixed_dates <- sapply(fixed_dates, function(fixed_date) {
    ifelse(
      !is.na(poi_sf$most_recent_burn),
      (year(fixed_date) - year(poi_sf$most_recent_burn)) * 12 + 
        (month(fixed_date) - month(poi_sf$most_recent_burn)),
      NA
    )
  })
  
  recommended_interval_months <- poi_sf$recommended_interval * 12
  
  FD_FLAG <- ifelse(
    fire_after_collection & rowSums(time_to_fixed_dates > recommended_interval_months, na.rm = TRUE) == length(fixed_dates),
    "Recovered",
    ifelse(
      fire_after_collection & rowSums(time_to_fixed_dates <= recommended_interval_months, na.rm = TRUE) > 0,
      "Recovering",
      "Unburnt"
    )
  )
  
  fixed_years <- c(2017, 2019, 2021, 2023)
  FD_3Y <- sapply(fixed_years, function(year) {
    window_start <- as.Date(paste0(year - 3, "-01-01"))
    window_end <- as.Date(paste0(year, "-01-01"))
    ifelse(
      !is.na(poi_sf$most_recent_burn) & 
        poi_sf$most_recent_burn >= window_start & 
        poi_sf$most_recent_burn < window_end,
      "Burnt in three years prior to era",
      "Unburnt in three years prior to era"
    )
  })
  
  poi_sf <- poi_sf %>%
    mutate(
      time_since_fire = time_since_fire,
      FD_FLAG = FD_FLAG,
      FD_2017_3Y = FD_3Y[, 1],
      FD_2019_3Y = FD_3Y[, 2],
      FD_2021_3Y = FD_3Y[, 3],
      FD_2023_3Y = FD_3Y[, 4]
    )
  
  return(poi_sf)
}

# Process fire data and calculate metrics
poi_sf <- process_fire_metrics(poi_sf)

# Calculate FD_FLAG and 3-year windows
fixed_dates <- as.Date(c("2017-01-01", "2019-01-01", "2021-01-01", "2023-01-01"))
poi_sf <- calculate_fd_flags(poi_sf, fixed_dates)

# # clean up intermediates
rm(fire_data)

### 2.4) Age Since Woody Disturbance  ----------------------------------------------------------------------------------

# 2.4.1) import woody disturbance rasters ====================
slats_disturbance <- terra::rast("spatial_inputs/SLATS_2023/woody_disturbance/DP_QLD_WOODY_AGE_2022_COG.tif") # time since clearing

# 2.4.2) Sample the raster in chunks ====================
# Initialize result vector
sampled_values <- rep(NA, nrow(poi_vect))

# Process in chunks
for (i in seq_len(n_chunks)) {
  start_idx <- (i - 1) * chunk_size + 1
  end_idx <- min(i * chunk_size, nrow(poi_vect))
  
  chunk <- poi_vect[start_idx:end_idx]
  sampled_values[start_idx:end_idx] <- terra::extract(slats_disturbance, chunk)[, 2]
  
  # Clean up to free memory
  rm(chunk)
  gc()
}

# Add results back to your sf object
poi_sf$ASWD_2022 <- sampled_values

# 2.4.3) Add in disturbance flag ====================
# Initialize new columns with NA
poi_sf$COLLECTION_YEAR <- NA_integer_
poi_sf$WD_YEAR <- NA_integer_
poi_sf$WD_TIMING <- NA_character_

# Process in chunks
for (i in seq_len(n_chunks)) {
  start_idx <- (i - 1) * chunk_size + 1
  end_idx <- min(i * chunk_size, nrow(poi_sf))
  
  # Get current chunk
  chunk <- poi_sf[start_idx:end_idx, ]
  
  # Convert dates and extract years
  chunk$COLLECTION_YEAR <- as.numeric(format(
    as.Date(chunk$COLLECTION_DATE, format = "%d/%m/%Y"), 
    "%Y"))
  
  # Identify rows to process (sampled_value between 1-30)
  process_rows <- which(chunk$ASWD_2022 >= 1 & chunk$ASWD_2022 <= 32)
  
  if (length(process_rows) > 0) {
    # Calculate WD_YEAR only for valid rows
    chunk$WD_YEAR[process_rows] <- 2022 - chunk$ASWD_2022[process_rows]
    
    # Determine WD timing for valid rows
    chunk$WD_TIMING[process_rows] <- case_when(
      chunk$WD_YEAR[process_rows] < chunk$COLLECTION_YEAR[process_rows] ~ "Before collection",
      chunk$WD_YEAR[process_rows] == chunk$COLLECTION_YEAR[process_rows] ~ "Same year",
      chunk$WD_YEAR[process_rows] > chunk$COLLECTION_YEAR[process_rows] ~ "After collection"
    )
  }
  
  # Write results back to main sf object
  poi_sf$COLLECTION_YEAR[start_idx:end_idx] <- chunk$COLLECTION_YEAR
  poi_sf$WD_YEAR[start_idx:end_idx] <- chunk$WD_YEAR
  poi_sf$WD_TIMING[start_idx:end_idx] <- chunk$WD_TIMING
  
  # Clean up
  rm(chunk)
  gc()
  
  message(sprintf("Processed chunk %d of %d (rows %d to %d) - %d valid rows", 
                  i, n_chunks, start_idx, end_idx, length(process_rows)))
}

# clean up
poi_sf <- poi_sf %>%
  select(-COLLECTION_YEAR, -WD_YEAR)
rm(slats_disturbance)


################################################################################################################################################################
################################################################################################################################################################
################################################################################################################################################################




### 2.1) Preclear ------------------------------------------------------------------------------------------------------

###QCHAT SOLUTION - UNTESTED!

library(sf)
library(terra)
library(dplyr)

# Path to the .gdb file
gdb_path <- "path_to_your_file.gdb"

# List all layers in the .gdb file
gdb_layers <- st_layers(gdb_path)$name
print(gdb_layers)  # Inspect available layers

# Convert poi_sf to terra vector for efficient processing
poi_vect <- vect(poi_sf)

# Set chunk size
chunk_size <- 50000
n_chunks <- ceiling(nrow(poi_vect) / chunk_size)

# Loop through each layer in the .gdb
for (layer_name in gdb_layers) {
  message("Processing layer: ", layer_name)
  
  # Load the current polygon layer
  polygon_layer <- st_read(gdb_path, layer = layer_name)
  
  # Skip non-polygon layers (if any)
  if (!inherits(polygon_layer, "sf") || !any(st_geometry_type(polygon_layer) %in% c("POLYGON", "MULTIPOLYGON"))) {
    message("Skipping non-polygon layer: ", layer_name)
    next
  }
  
  # Convert polygon layer to terra format
  polygon_vect <- vect(polygon_layer)
  
  # Initialise a list to store results for this layer
  results_list <- vector("list", n_chunks)
  
  # Process poi_sf in chunks
  for (i in seq_len(n_chunks)) {
    start_idx <- (i - 1) * chunk_size + 1
    end_idx <- min(i * chunk_size, nrow(poi_vect))
    
    # Extract chunk of points
    chunk <- poi_vect[start_idx:end_idx, ]
    
    # Perform spatial join (intersect points with polygons)
    joined <- terra::intersect(chunk, polygon_vect)
    
    # Extract relevant attributes from the polygon layer
    if (!is.null(joined)) {
      joined_df <- as.data.frame(joined)
      results_list[[i]] <- joined_df
    }
    
    # Clean up
    rm(chunk, joined)
    gc()
    
    message(sprintf("  Chunk %d/%d processed for layer: %s", i, n_chunks, layer_name))
  }
  
  # Combine all results for this layer into a single data frame
  results_df <- do.call(rbind, results_list)
  
  # Add results to poi_sf
  if (!is.null(results_df) && nrow(results_df) > 0) {
    col_prefix <- gsub("[^a-zA-Z0-9]", "_", layer_name)  # Sanitize layer name for column prefix
    results_df <- results_df %>%
      select(-geometry)  # Remove geometry column if present
    colnames(results_df) <- paste0(col_prefix, "_", colnames(results_df))  # Prefix column names
    
    poi_sf <- poi_sf %>%
      left_join(results_df, by = "fid")  # Replace with your unique ID column
  }
  
  # Clean up after processing the layer
  rm(polygon_layer, polygon_vect, results_list, results_df)
  gc()
  
  message("Finished processing layer: ", layer_name)
}

# Final clean-up
rm(gdb_layers)
gc()



































# 2.1.1) Create File Inventory
# List all available years
preclear_dirs <- dir_ls("spatial_inputs/Preclear", regexp = "Preclear_v\\d+-\\d+_\\d{4}")

# Extract years and paths
preclear_years <- tibble(
  path = preclear_dirs,
  year = str_extract(path, "\\d{4}$")
) %>%
  arrange(desc(year))  # Process newest first (often better quality)

# 2.1.2) Memory-efficient processing function
process_preclear <- function(year_path, points_sf) {
  # Read with geometry repair
  layer <- st_read(
    file.path(year_path, "data.gdb"),
    quiet = TRUE,
    stringsAsFactors = FALSE,
    promote_to_multi = TRUE  # Force multi-geometries
  ) %>% 
    st_cast("MULTIPOLYGON") %>%  # Convert all to supported type
    sf::st_make_valid() %>%  # More robust than sf::st_make_valid
    st_simplify(preserveTopology = TRUE, dTolerance = 0.1) %>%  # Reduce complexity
    select(any_of(c("re1", "dbvg1m")))
  
  # Skip if no valid geometries remain
  if (nrow(layer) == 0 || all(is.na(st_dimension(layer)))) return(NULL)
  
  # Spatial join with error handling
  result <- tryCatch({
    st_join(
      points_sf,
      layer,
      join = st_intersects,
      left = TRUE,
      largest = TRUE
    ) %>%
      st_drop_geometry() %>%
      select(ends_with("re1"), ends_with("dbvg1m")) %>%
      rename_with(~ paste0("preclear_", .x, "_", str_extract(year_path, "\\d{4}$")))
  }, error = function(e) {
    message("Failed on ", year_path, ": ", e$message)
    NULL
  })
  
  rm(layer); gc()
  return(result)
}

# Process sequentially (more reliable than parallel for problematic geometries)
preclear_results <- map(
  preclear_years$path, 
  ~ process_preclear(., poi_sf),
  .progress = TRUE
) %>% 
  compact() %>%
  reduce(left_join, by = "fid")  # Adjust 'fid' to your actual ID column

# 2.1.3) Merge final results
poi_final <- poi_sf %>%
  st_drop_geometry() %>%
  left_join(preclear_results, by = "fid")

# Force final cleanup
rm(preclear_results); gc()









# Ensure poi is in the same CRS as the vector data
poi_sf <- st_as_sf(poi, coords = c("x_gda94", "y_gda94"), crs = 4283) 

# Convert geometry and make valid
pre_clear <- pre_clear %>%
  st_cast("MULTIPOLYGON") %>%  # Convert geometry type
  st_make_valid()  # Ensure validity

# Spatial join to extract attributes from 'preclear' at poi locations
poi_sampled <- st_join(poi_sf, pre_clear, join = st_within)  # Use st_intersects if points can be on boundaries






#------------------------------------------ 2) Training Data Compiler -------------------------------------------------#
# 2.1) Mutate and join training data sources

# 2.2) Remove duplicates and ensure most recent site visit

# 2.3) Clean dataframe

# 2.4) Spatial intersect of RE and flag valid/mismatch RE

# 2.5) Create date + accuracy cuts of dataset to run through spatial auditing
# past was limited to 1995, but look at using reference condition if in PA and no disturbance
# pass accuracy was 200 m but explore higher threshold to see if anything else of value appears
# 

#------------------------------------------ 3) Training Data Compiler -------------------------------------------------#
# 3.1) Flag proximity trigger for roads, re boundary, remnant, hvr, other td points
# create 90 m buffer around all points using sentinel grid window 9x9
# intersect point assessment window with trigger layers
# look at eight surrounding 9x9 assessment windows and check triggers to suggest point relocation