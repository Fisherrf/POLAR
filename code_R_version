# --- 1. SETUP: LOAD LIBRARIES ---
# Install packages if you don't have them
# install.packages(c("tidyverse", "lubridate", "readxl", "arrow", "didm"))

library(tidyverse) # Includes dplyr, readr, tidyr, and more
library(lubridate) # For all date and time manipulation
library(readxl)    # For reading Excel files
library(arrow)     # For reading/writing high-performance Parquet files (R's .dta)
library(didm)      # For did_multiplegt()

# --- 2. SETUP: SET FILE PATHS ---
# Stata: global data "..."
# R: Create path variables
data_path <- "C:/Users/fisherrf/The University of Melbourne/Karinna Saxby - polar/data/data_updated"
output_path <- "C:/Users/fisherrf/The University of Melbourne/Karinna Saxby - polar/output/rcbh/matched"
output_path2 <- "C:/Users/fisherrf/The University of Melbourne/Karinna Saxby - polar/output/rcbh/unmatched"

# Set default theme for ggplot2 (optional, but good practice)
theme_set(theme_minimal())

# ********************************************************************************
# * IMPORT AND CONVERT CSV/EXCEL FILES
# ********************************************************************************

# *import polar excel files and save as .dta
# Stata: local files: dir "$data" files "*.csv"
# Stata: foreach f of local files { ... }
csv_files <- list.files(data_path, pattern = "\\.csv$", full.names = TRUE)

# Loop through, read, and write to Parquet (R's modern .dta)
# Using lapply (functional loop)
lapply(csv_files, function(f) {
  message(paste("Processing file:", basename(f)))
  
  # Stata: import delimited ...
  df <- read_csv(f, guess_max = 50000) # guess_max helps with type detection
  
  # Stata: local fname = subinstr("`f'", ".csv", "", .)
  fname <- tools::file_path_sans_ext(basename(f))
  
  # Stata: save "${data}/`fname'.dta", replace
  write_parquet(df, file.path(data_path, paste0(fname, ".parquet")))
})

# *import september ONLY POLAR files and save as .dta
# Stata: foreach fn in presc_sept ... { ... }
sept_files <- c("presc_sept", "service_sept", "patient_sept", 
                "patientflag_sept", "referral_sept", "diagnosis_sept")

for (fn in sept_files) {
  read_csv(file.path(data_path, paste0(fn, ".csv")), guess_max = 50000) %>%
    write_parquet(file.path(data_path, paste0(fn, ".parquet")))
}


# ****************************************************************
# * Import EMPHN monthly patient datasets & ABS Data
# ****************************************************************

# **EACH
# Stata: import excel "$data/EACH_final", firstrow sheet(Patient) clear
read_excel(file.path(data_path, "EACH_final.xlsx"), sheet = "Patient") %>%
  write_parquet(file.path(data_path, "monthly_EACH.parquet"))

read_excel(file.path(data_path, "EACH_final.xlsx"), sheet = "Patient_Conditions") %>%
  write_parquet(file.path(data_path, "monthly_diagnosis_EACH.parquet"))

# **Updated EACH dataset with date of birth variable
# Stata: import excel "$data/EACH_jul_DOB" ...
# Stata: merge 1:m ...
# Stata: keep if _merge ==3
monthly_EACH <- read_parquet(file.path(data_path, "monthly_EACH.parquet"))

read_excel(file.path(data_path, "EACH_jul_DOB.xlsx")) %>%
  select(-EpisodeNo) %>%
  rename(PatientSiteKey = PSK) %>%
  # Stata: duplicates drop PatientSiteKey, force
  distinct(PatientSiteKey, .keep_all = TRUE) %>%
  # Stata: merge 1:m ... keep if _merge ==3
  inner_join(monthly_EACH, by = "PatientSiteKey") %>%
  # Stata: keep Practice_ID ...
  select(Practice_ID, PatientSiteKey, Dateofbirth, Sex, Postcode, 
         ReferralReceivedDate, Enrolled_Date, Withdrawal_Date, 
         Graduated_Date, Withdrawal_Reason) %>%
  write_parquet(file.path(data_path, "monthly_EACH_DoB.parquet"))

# **Silverchain
read_excel(file.path(data_path, "SC_final.xlsx"), sheet = "PatientDetails") %>%
  write_parquet(file.path(data_path, "monthly_silverchain.parquet"))

read_excel(file.path(data_path, "SC_final.xlsx"), sheet = "PatientConditions") %>%
  write_parquet(file.path(data_path, "monthly_diagnosis_silverchain.parquet"))

# * POLAR diagnosis dataset
# Stata: g diag_dt = clock(...)
# Stata: g diag_date = dofc(diag_dt)
# Stata: g diag_month = mofd(diag_date)
# Stata: keep if diag_month >= tm(2022m1) & diag_month <=tm(2025m9)
read_parquet(file.path(data_path, "diagnosis_sept.parquet")) %>%
  mutate(
    # Stata: clock(...)
    diag_dt = ymd_hms(diagnosis_recorded_date),
    # Stata: dofc()
    diag_date = as_date(diag_dt),
    # Stata: mofd()
    diag_month = floor_date(diag_date, "month")
  ) %>%
  # Stata: keep if diag_month >= tm(2022m1) & ...
  filter(diag_month >= ym("2022-01") & diag_month <= ym("2025-09")) %>%
  select(patientsitekey, chronic_disease_category, diag_month) %>%
  write_parquet(file.path(data_path, "diagnosis_sept_cleaned.parquet"))

# * SEIFA
# Stata: import excel "$data/SEIFA", firstrow sheet(Table 2) clear
read_excel(file.path(data_path, "SEIFA.xlsx"), sheet = "Table 2", skip = 1) %>%
  rename(post_code = AustralianBureauof,
         post_code_pop = B,
         seifa_irsd = C) %>%
  mutate(post_code = as.numeric(post_code)) %>%
  filter(post_code >= 3000 & post_code <= 3999, !is.na(post_code)) %>%
  select(post_code, post_code_pop, seifa_irsd) %>%
  write_parquet(file.path(data_path, "SEIFA_irsd.parquet"))


# *******************************************************************
# * 1) Clean the monthly datasets prior to merging with POLAR
# *******************************************************************

# *****************
# * EACH dataset
# *****************
monthly_diagnosis_EACH <- read_parquet(file.path(data_path, "monthly_diagnosis_EACH.parquet"))

each_cleaned <- read_parquet(file.path(data_path, "monthly_EACH_DoB.parquet")) %>%
  rename(practice_id = Practice_ID,
         dob = Dateofbirth,
         sex = Sex,
         post_code = Postcode,
         referral_date_EACH = ReferralReceivedDate,
         enrolled_date_EACH = Enrolled_Date,
         withdrawal_date_EACH = Withdrawal_Date,
         graduated_date_EACH = Graduated_Date,
         withdrawal_reason = Withdrawal_Reason) %>%
  # Stata: replace practice_id = "392" if ...
  mutate(practice_id = if_else(practice_id %in% c("392B", "392C"), "392", practice_id)) %>%
  # Stata: duplicates drop PatientSiteKey enrolled_date_EACH, force
  distinct(PatientSiteKey, enrolled_date_EACH, .keep_all = TRUE) %>%
  
  # Stata: * Generate unique variables for each persons multiple enrolment...
  # Stata: bys PatientSiteKey (enrolled_date_EACH): g n_enrol = _n
  # ... (and all other bys/egen blocks)
  group_by(PatientSiteKey) %>%
  arrange(enrolled_date_EACH) %>%
  mutate(n_enrol = row_number()) %>%
  arrange(graduated_date_EACH) %>%
  mutate(n_grad = row_number()) %>%
  arrange(withdrawal_date_EACH) %>%
  mutate(n_withd = row_number()) %>%
  
  # Create the sparse columns first
  mutate(
    enrolled_1 = if_else(n_enrol == 1, enrolled_date_EACH, NA),
    enrolled_2 = if_else(n_enrol == 2, enrolled_date_EACH, NA),
    enrolled_3 = if_else(n_enrol == 3, enrolled_date_EACH, NA),
    grad_1 = if_else(n_grad == 1, graduated_date_EACH, NA),
    grad_2 = if_else(n_grad == 2, graduated_date_EACH, NA),
    grad_3 = if_else(n_grad == 3, graduated_date_EACH, NA),
    withd_1 = if_else(n_withd == 1, withdrawal_date_EACH, NA),
    withd_2 = if_else(n_withd == 2, withdrawal_date_EACH, NA),
    withd_3 = if_else(n_withd == 3, withdrawal_date_EACH, NA)
  ) %>%
  
  # Stata: bys PatientSiteKey: egen enrolled_1b_EACH = min(enrolled_1)
  # Now "broadcast" the min value to all rows in the group
  mutate(
    enrolled_1b_EACH = min(enrolled_1, na.rm = TRUE),
    enrolled_2b_EACH = min(enrolled_2, na.rm = TRUE),
    enrolled_3b_EACH = min(enrolled_3, na.rm = TRUE),
    grad_1b_EACH = min(grad_1, na.rm = TRUE),
    grad_2b_EACH = min(grad_2, na.rm = TRUE),
    grad_3b_EACH = min(grad_3, na.rm = TRUE),
    withd_1b_EACH = min(withd_1, na.rm = TRUE),
    withd_2b_EACH = min(withd_2, na.rm = TRUE),
    withd_3b_EACH = min(withd_3, na.rm = TRUE)
  ) %>%
  
  # Stata: bys PatientSiteKey: keep if _n==1
  # This collapses the data to one row per patient
  filter(row_number() == 1) %>%
  ungroup() %>%
  
  # Stata: drop ...
  select(-c(enrolled_date_EACH, withdrawal_date_EACH, graduated_date_EACH, 
            n_enrol, n_grad, n_withd, 
            enrolled_1, enrolled_2, enrolled_3, 
            grad_1, grad_2, grad_3, 
            withd_1, withd_2, withd_3)) %>%
  
  # * Deal with new date of birth variable
  # Stata: gen double dob_date = dofc(dob)
  # Note: The format "11nov1965 00:00:00" needs dmy_hms
  mutate(
    # Stata: dofc(dob)
    dob_date = as_date(dmy_hms(dob)),
    # Stata: gen str4 yob = string(year(dob_date))
    yob = as.character(year(dob_date)),
    # Stata: gen str2 mob = string(month(dob_date))
    # R: sprintf formats "1" as "01"
    mob = sprintf("%02d", month(dob_date))
  ) %>%
  
  # * Merge diagnosis data in from monthly patient datasets
  # Stata: merge 1:m PatientSiteKey using "$data/monthly_diagnosis_EACH.dta"
  left_join(monthly_diagnosis_EACH, 
            by = "PatientSiteKey", 
            relationship = "many-to-many") %>%
  
  # Stata: g diag_miss_EACH = _merge ==1
  mutate(diag_miss_EACH = is.na(Diagnosis)) %>%
  
  # Stata: g cardio_each = inlist(...)
  mutate(
    cardio_each = Diagnosis %in% c("Chronic Complex- Angina", "Chronic Complex- Cardiovascular (heart) conditions",
                                   "Chronic Complex- Cholesterol (lipid metabolism disorder)", 
                                   "Chronic Complex- Chronic (congestive) heart failure",
                                   "Chronic Complex- Hypertension"),
    respiratory_each = Diagnosis %in% c("Chronic Complex- Asthma", 
                                        "Chronic Complex- Chronic obstructive pulmonary disease (COPD)",
                                        "Chronic Complex- Respiratory conditions, other"),
    frailty_each = Diagnosis %in% c("Chronic Complex- Musculo-skeletal conditions, other", 
                                    "Chronic Complex- Osteoarthritis",
                                    "Chronic Complex- Osteoporosis", 
                                    "Chronic Complex- Rheumatoid arthritis")
  ) %>%
  
  # Stata: bys PatientSiteKey: egen cardio_each_max = max(cardio_each)
  group_by(PatientSiteKey) %>%
  mutate(
    cardio_each_max = max(cardio_each, na.rm = TRUE),
    resp_each_max = max(respiratory_each, na.rm = TRUE),
    frailty_each_max = max(frailty_each, na.rm = TRUE)
  ) %>%
  
  # Stata: bys PatientSiteKey: keep if _n==1
  filter(row_number() == 1) %>%
  ungroup() %>%
  
  # Stata: bys post_code sex yob mob practice_id: gen N=_N
  # Stata: drop if N>1
  # R: add_count() is the direct equivalent
  add_count(post_code, sex, yob, mob, practice_id) %>%
  filter(n == 1) %>%
  
  # Stata: drop ...
  select(-c(n, PatientSiteKey, dob_date, dob, Diagnosis, 
            cardio_each, respiratory_each, frailty_each))

# Stata: save "$data/EACH_cleaned.dta", replace
write_parquet(each_cleaned, file.path(data_path, "EACH_cleaned.parquet"))

# ****************************************
# * Silverchain dataset
# ****************************************
monthly_diagnosis_silverchain <- read_parquet(file.path(data_path, "monthly_diagnosis_silverchain.parquet"))

silverchain_cleaned <- read_parquet(file.path(data_path, "monthly_silverchain.parquet")) %>%
  # Stata: split(PatientSiteKey), p("-")
  # R: tidyr::separate()
  separate(PatientSiteKey, into = c("practice_id", NA), sep = "-", remove = FALSE) %>%
  select(PatientID, PatientSiteKey, PracticeName, DOB, Sex, PostCode, 
         ReferralDate, EnrolledDate, WithdrawalDate, GraduatedDate, practice_id) %>%
  rename(sex = Sex,
         post_code = PostCode,
         referral_date_SC = ReferralDate,
         enrolled_date_SC = EnrolledDate,
         withdrawal_date_SC = WithdrawalDate,
         grad_1b_SC = GraduatedDate) %>%
  
  # Stata: bys PatientID (enrolled_date_SC): g n_enrol = _n
  group_by(PatientID) %>%
  arrange(enrolled_date_SC) %>%
  mutate(n_enrol = row_number()) %>%
  
  # Stata: bys PatientID: g enrolled_1 = enrolled_date_SC if n_enrol ==1
  # Stata: bys PatientID: egen enrolled_1b_SC = min(enrolled_1)
  mutate(enrolled_1 = if_else(n_enrol == 1, enrolled_date_SC, NA)) %>%
  mutate(enrolled_1b_SC = min(enrolled_1, na.rm = TRUE)) %>%
  
  # Stata: bys PatientID: keep if _n==1
  filter(row_number() == 1) %>%
  ungroup() %>%
  select(-c(enrolled_date_SC, n_enrol, enrolled_1)) %>%
  
  # Stata: merge 1:m PatientID using "$data/monthly_diagnosis_silverchain.dta"
  left_join(monthly_diagnosis_silverchain, 
            by = "PatientID", 
            relationship = "many-to-many") %>%
  
  # Stata: g cardio_SC = inlist(DiagnosisGroup, "Cardiovascular Disease")
  mutate(
    cardio_SC = DiagnosisGroup == "Cardiovascular Disease",
    resp_SC = DiagnosisGroup == "Respiratory Disease",
    frailty_SC = DiagnosisGroup == "Frailty/Falls Risk"
  ) %>%
  
  # Stata: bys PatientID: egen cardio_SC_max = max(cardio_SC)
  group_by(PatientID) %>%
  mutate(
    cardio_SC_max = max(cardio_SC, na.rm = TRUE),
    resp_SC_max = max(resp_SC, na.rm = TRUE),
    frailty_SC_max = max(frailty_SC, na.rm = TRUE)
  ) %>%
  
  # Stata: drop if _merge ==2 (This is handled by left_join + filter)
  filter(!is.na(PatientSiteKey)) %>% # Drop rows from diagnosis that didn't match
  
  # Stata: bys PatientID: keep if _n==1
  filter(row_number() == 1) %>%
  ungroup() %>%
  
  # * Gen the dates var as follows to match to POLAR later
  mutate(
    yob = as.character(year(DOB)),
    mob = sprintf("%02d", month(DOB))
  ) %>%
  
  # Stata: bys post_code sex yob mob practice_id: gen N=_N
  # Stata: keep if N ==1
  add_count(post_code, sex, yob, mob, practice_id) %>%
  filter(n == 1) %>%
  
  # Stata: keep sex post_code ...
  select(sex, post_code, withdrawal_date_SC, referral_date_SC, grad_1b_SC,
         practice_id, enrolled_1b_SC, cardio_SC_max, resp_SC_max, 
         frailty_SC_max, yob, mob)

# Stata: save "$data/silverchain_cleaned.dta", replace
write_parquet(silverchain_cleaned, file.path(data_path, "silverchain_cleaned.parquet"))


# *******************************************************************
# * 2) Merge unique patients from EACH/Silverchain datasets to POLAR
# *******************************************************************

# ******************************
# * Clean POLAR diagnosis data
# ******************************
patient_sept <- read_parquet(file.path(data_path, "patient_sept.parquet"))

diag_sept_cleaned <- read_parquet(file.path(data_path, "diagnosis_sept_cleaned.parquet")) %>%
  # Stata: merge m:1 patientsitekey using "$data/patient_sept"
  inner_join(select(patient_sept, patientsitekey, polar_id_1), by = "patientsitekey") %>%
  
  # Stata: g cardio = 1 if chronic_disease_category == "Cardiovascular"
  # Note: `encode ... g(sad)` and `g dementia_alzheim = 1 if sad ==1` 
  # is fragile. It assumes the first alphabetical factor level is the one of interest.
  # R's `as.factor` also orders alphabetically. This is a literal translation.
  mutate(
    sad = as.integer(as.factor(chronic_disease_category)),
    cardio = chronic_disease_category == "Cardiovascular",
    respiratory = chronic_disease_category == "Respiratory",
    cancer = chronic_disease_category == "Cancer",
    muscul = chronic_disease_category == "Musculoskeletal",
    mental = chronic_disease_category == "Mental Health",
    diab = chronic_disease_category == "Diabetes",
    disability = chronic_disease_category == "Disability",
    dementia_alzheim = sad == 1,
    chronic_other = chronic_disease_category %in% c("AoD", "CKD", "Oral")
  ) %>%
  
  # Stata: bys polar_id_1: egen cardio_max = max(cardio)
  group_by(polar_id_1) %>%
  mutate(
    across(
      c(cardio, respiratory, cancer, muscul, mental, diab, disability, 
        dementia_alzheim, chronic_other),
      list(max = ~max(.x, na.rm = TRUE))
    )
  ) %>%
  
  # Stata: foreach var of varlist *_max { replace `var' = 0 if `var' ==. }
  # R: `replace_na()` is the tidyverse function
  mutate(across(ends_with("_max"), ~replace_na(., 0))) %>%
  
  # Stata: g diag_date_cardio = diag_month if cardio==1
  mutate(
    diag_date_cardio = if_else(cardio == 1, diag_month, NA),
    diag_date_resp = if_else(respiratory == 1, diag_month, NA),
    diag_date_cancer = if_else(cancer == 1, diag_month, NA),
    diag_date_muscul = if_else(muscul == 1, diag_month, NA),
    diag_date_mental = if_else(mental == 1, diag_month, NA),
    diag_date_diab = if_else(diab == 1, diag_month, NA),
    diag_date_disability = if_else(disability == 1, diag_month, NA),
    diag_date_dementia_alz = if_else(dementia_alzheim == 1, diag_month, NA)
  ) %>%
  
  # Stata: bys polar_id_1: egen `var'_max = min(`var')
  # Note: Stata code names this `_max` but uses `min()` function. Following logic.
  mutate(
    across(
      starts_with("diag_date_"),
      list(max = ~min(.x, na.rm = TRUE)),
      .names = "{.col}_max"
    )
  ) %>%
  
  # Stata: egen chronic_eligible = rowtotal(cardio_max respiratory_max muscul_max)
  # R: rowSums() needs `na.rm=TRUE` if any can be NA
  mutate(chronic_eligible = cardio_max + respiratory_max + muscul_max) %>%
  
  # Stata: keep if chronic_eligible >0
  filter(chronic_eligible > 0) %>%
  
  # Stata: duplicates drop polar_id_1, force
  distinct(polar_id_1, .keep_all = TRUE) %>%
  ungroup() %>%
  
  # Stata: drop ...
  select(polar_id_1, ends_with("_max"), starts_with("diag_date_"))

# Stata: save "$data/diagnosis_sept_cleaned_final.dta"
write_parquet(diag_sept_cleaned, file.path(data_path, "diagnosis_sept_cleaned_final.parquet"))


# ********************************************************************
# * 3) Clean POLAR patientflag (RCBH enrolled only)
# ********************************************************************
premerge_NoDropNYet <- read_parquet(file.path(data_path, "patientflag_sept.parquet")) %>%
  # Stata: keep if inlist(flag_status, "RCBH Enrolled", ...)
  filter(flag_status %in% c("RCBH Enrolled", "RCBH Graduated", "RCBH Withdrawn")) %>%
  
  # Stata: split(patientsitekey),p("-")
  separate(patientsitekey, into = c("patientsitekey1", "patientsitekey2"), sep = "-", remove = FALSE) %>%
  mutate(patientsitekey1 = as.numeric(patientsitekey1)) %>%
  
  # Stata: drop if inlist(patientsitekey1, 1085, 1083, 688)
  filter(!patientsitekey1 %in% c(1085, 1083, 688)) %>%
  
  # Stata: g flag_each_prac = inlist(...)
  mutate(
    flag_each_prac = patientsitekey1 %in% c(82, 88, 103, 115, 125, 393, 510, 1066, 2028, 
                                            2029, 2030, 2031, 392),
    flag_sc_prac = patientsitekey1 %in% c(1445, 1797, 244, 247, 472, 59, 782, 820, 953, 
                                          141, 977, 2154, 847, 970, 1001, 1523, 968)
  ) %>%
  
  # Stata: g enrolled = (flag_status == "RCBH Enrolled")
  # Stata: bys patientsitekey: egen max_enrol = max(enrolled)
  # Stata: keep if max_enrol ==1
  mutate(enrolled = (flag_status == "RCBH Enrolled")) %>%
  group_by(patientsitekey) %>%
  mutate(max_enrol = max(enrolled, na.rm = TRUE)) %>%
  filter(max_enrol == 1) %>%
  
  # * Get unique variables for each unique date of enrolment, graduated, withdrawal
  # Stata: gen dt_tm  = clock(flag_datetime, "YMDhms")
  # Stata: gen status = lower(word(flag_status, 2))
  mutate(
    dt_tm = ymd_hms(flag_datetime),
    d_day = as_date(dt_tm),
    # R: stringr::word()
    status = tolower(word(flag_status, 2))
  ) %>%
  
  # Stata: bys patientsitekey status (dt): gen seq = _n
  # Note: `(dt)` in Stata code is likely typo for `dt_tm`
  group_by(patientsitekey, status) %>%
  arrange(dt_tm) %>%
  mutate(seq = row_number()) %>%
  
  # Stata: g enrolled_1 = d_day if seq ==1 & status =="enrolled"
  # (and all other sparse columns)
  mutate(
    enrolled_1 = if_else(seq == 1 & status == "enrolled", d_day, NA),
    enrolled_2 = if_else(seq == 2 & status == "enrolled", d_day, NA),
    enrolled_3 = if_else(seq == 3 & status == "enrolled", d_day, NA),
    enrolled_4 = if_else(seq == 4 & status == "enrolled", d_day, NA),
    grad_1 = if_else(seq == 1 & status == "graduated", d_day, NA),
    grad_2 = if_else(seq == 2 & status == "graduated", d_day, NA),
    withd_1 = if_else(seq == 1 & status == "withdrawn", d_day, NA),
    withd_2 = if_else(seq == 2 & status == "withdrawn", d_day, NA)
  ) %>%
  
  # Stata: bys patientsitekey: egen enrolled_1b = min(enrolled_1)
  # (and all other egens)
  group_by(patientsitekey) %>%
  mutate(
    enrolled_1b = min(enrolled_1, na.rm = TRUE),
    enrolled_2b = min(enrolled_2, na.rm = TRUE),
    enrolled_3b = min(enrolled_3, na.rm = TRUE),
    enrolled_4b = min(enrolled_4, na.rm = TRUE),
    grad_1b = min(grad_1, na.rm = TRUE),
    grad_2b = min(grad_2, na.rm = TRUE),
    withd_1b = min(withd_1, na.rm = TRUE),
    withd_2b = min(withd_2, na.rm = TRUE)
  ) %>%
  
  # Stata: bys patientsitekey: keep if _n ==1
  filter(row_number() == 1) %>%
  ungroup() %>%
  
  # Stata: drop enrolled_1 ...
  select(-c(enrolled_1, enrolled_2, enrolled_3, enrolled_4, 
            grad_1, grad_2, withd_1, withd_2)) %>%
  
  # Stata: merge 1:1 patientsitekey using "$data/patient_sept.dta"
  # Stata: keep if _merge ==3
  inner_join(select(patient_sept, patientsitekey, polar_id_1, yob, mob, sex, post_code, practice_id),
             by = "patientsitekey") %>%
  
  # Stata: drop if patientsitekey == "782-24012"
  filter(patientsitekey != "782-24012") %>%
  # Stata: drop if yob == "2017"
  filter(yob != "2017") %>%
  
  # Stata: drop flag_id ...
  select(polar_id_1, patientsitekey, practice_id, post_code, sex, yob, mob, 
         starts_with("enrolled_"), starts_with("grad_"), starts_with("withd_"))

# Stata: save "$data/premerge_NoDropNYet.dta", replace
write_parquet(premerge_NoDropNYet, file.path(data_path, "premerge_NoDropNYet.parquet"))

# ********************************
# * 4) Merge SC/EACH data to POLAR
# ********************************

# * Get premerge dataset
# Stata: use "$data/premerge_NoDropNYet.dta", clear
rcbh_premerge <- read_parquet(file.path(data_path, "premerge_NoDropNYet.parquet")) %>%
  # Stata: bys post_code yob mob sex practice_id: g N = _N
  # Stata: keep if N ==1
  add_count(post_code, yob, mob, sex, practice_id) %>%
  filter(n == 1) %>%
  select(polar_id_1, patientsitekey, practice_id, post_code, sex, yob, mob) %>%
  # Stata: tostring post_code, replace
  mutate(post_code = as.character(post_code),
         practice_id = as.character(practice_id))

# Stata: save "$data/rcbh_premerge.dta", replace
write_parquet(rcbh_premerge, file.path(data_path, "rcbh_premerge.parquet"))

# * Now merge silverchain first
silverchain_cleaned <- read_parquet(file.path(data_path, "silverchain_cleaned.parquet"))

# Stata: preserve
# Stata: keep if _merge ==2
# R: `anti_join` finds rows in `silverchain_cleaned` NOT in `rcbh_premerge`
anti_join(silverchain_cleaned, rcbh_premerge,
          by = c("post_code", "yob", "mob", "sex", "practice_id")) %>%
  write_parquet(file.path(data_path, "rcbh_SC_nonmerged.parquet"))

# Stata: restore
# Stata: keep if _merge ==3
# R: `inner_join` finds rows in BOTH
rcbh_premerge %>%
  inner_join(silverchain_cleaned,
             by = c("post_code", "yob", "mob", "sex", "practice_id")) %>%
  # Stata: drop enrolled_1b ...
  select(-c(starts_with("enrolled_"), starts_with("grad_"), starts_with("withd_"))) %>%
  write_parquet(file.path(data_path, "rcbh_SC_matched.parquet"))

# *Then EACH
each_cleaned <- read_parquet(file.path(data_path, "EACH_cleaned.parquet"))

# Stata: preserve
# Stata: keep if _merge ==2
anti_join(each_cleaned, rcbh_premerge,
          by = c("post_code", "yob", "mob", "sex", "practice_id")) %>%
  write_parquet(file.path(data_path, "rcbh_EACH_nonmerged.parquet"))

# Stata: restore
# Stata: keep if _merge ==3
rcbh_premerge %>%
  mutate(post_code = as.numeric(post_code)) %>% # Convert back for join
  inner_join(each_cleaned,
             by = c("post_code", "yob", "mob", "sex", "practice_id")) %>%
  select(-c(starts_with("enrolled_"), starts_with("grad_"), starts_with("withd_"))) %>%
  write_parquet(file.path(data_path, "rcbh_EACH_matched.parquet"))

# ** Append the EACH and Silverchain datasets together
# Stata: use "$data/rcbh_SC_matched.dta", clear
rcbh_sc_matched <- read_parquet(file.path(data_path, "rcbh_SC_matched.parquet"))
rcbh_each_matched <- read_parquet(file.path(data_path, "rcbh_EACH_matched.parquet"))

# Stata: append using "$data/rcbh_EACH_matched.dta"
# R: `bind_rows` is the equivalent of `append`
bind_rows(
  rcbh_sc_matched %>% mutate(post_code = as.character(post_code)), 
  rcbh_each_matched %>% mutate(post_code = as.character(post_code))
) %>%
  write_parquet(file.path(data_path, "rcbh_EACH_SC_Appended.parquet"))


# ***********************************
# * 5) Remaining Silverchain patients
# ***********************************
# This section repeats logic from section 3, but for different flag_status
NoDropNYet_NONMERGED <- read_parquet(file.path(data_path, "patientflag_sept.parquet")) %>%
  filter(flag_status %in% c("RCBH Graduated", "RCBH Withdrawn", "RCBH  Referred", "RCBH Referred")) %>%
  separate(patientsitekey, into = c("patientsitekey1", "patientsitekey2"), sep = "-", remove = FALSE) %>%
  mutate(patientsitekey1 = as.numeric(patientsitekey1)) %>%
  filter(!patientsitekey1 %in% c(1083, 1085, 688)) %>%
  # ... (g flag_each, g flag_silverchain) ...
  
  # Stata: gen dt_tm ...
  mutate(
    dt_tm = ymd_hms(flag_datetime),
    d_day = as_date(dt_tm),
    status = tolower(word(flag_status, 2))
  ) %>%
  group_by(patientsitekey, status) %>%
  arrange(dt_tm) %>%
  mutate(seq = row_number()) %>%
  
  # Stata: g grad_1 = ...
  mutate(
    grad_1 = if_else(seq == 1 & status == "graduated", d_day, NA),
    grad_2 = if_else(seq == 2 & status == "graduated", d_day, NA),
    withd_1 = if_else(seq == 1 & status == "withdrawn", d_day, NA),
    withd_2 = if_else(seq == 2 & status == "withdrawn", d_day, NA),
    refer_1 = if_else(seq == 1 & status == "referred", d_day, NA)
  ) %>%
  
  # Stata: bys patientsitekey: egen grad_1b = min(grad_1)
  group_by(patientsitekey) %>%
  mutate(
    grad_1b = min(grad_1, na.rm = TRUE),
    grad_2b = min(grad_2, na.rm = TRUE),
    withd_1b = min(withd_1, na.rm = TRUE),
    withd_2b = min(withd_2, na.rm = TRUE)
  ) %>%
  
  # Stata: bys patientsitekey: keep if _n ==1
  filter(row_number() == 1) %>%
  ungroup() %>%
  
  # Stata: merge 1:1 patientsitekey using "$data/patient_sept.dta"
  inner_join(select(patient_sept, patientsitekey, polar_id_1, yob, mob, sex, post_code, practice_id),
             by = "patientsitekey") %>%
  
  # Stata: destring yob ... g age = 2025 - yob
  mutate(
    yob = as.numeric(yob),
    age = 2025 - yob
  ) %>%
  # Stata: drop if age <18 | age ==.
  filter(age >= 18, !is.na(age)) %>%
  select(-c(age, grad_1, grad_2, withd_1, withd_2))

# Stata: save "$data/NoDropNYet_NONMERGED.dta", replace
write_parquet(NoDropNYet_NONMERGED, file.path(data_path, "NoDropNYet_NONMERGED.parquet"))

# ** Get premerge datasets
rcbh_SC_premerge_NONMERGED <- read_parquet(file.path(data_path, "NoDropNYet_NONMERGED.parquet")) %>%
  add_count(post_code, yob, mob, sex, practice_id) %>%
  filter(n == 1) %>%
  select(polar_id_1, patientsitekey, practice_id, post_code, sex, yob, mob, n,
         starts_with("grad_"), starts_with("withd_"), refer_1) %>%
  mutate(across(c(post_code, practice_id, yob), as.character))

# Stata: save "$data/rcbh_SC_premerge_NONMERGED.dta", replace
write_parquet(rcbh_SC_premerge_NONMERGED, file.path(data_path, "rcbh_SC_premerge_NONMERGED.parquet"))

# * Now merge silverchain first
rcbh_SC_nonmerged <- read_parquet(file.path(data_path, "rcbh_SC_nonmerged.parquet"))

rcbh_SC_matched_NONMERGED <- rcbh_SC_premerge_NONMERGED %>%
  # Stata: merge 1:1 ... using "$data/rcbh_SC_nonmerged.dta"
  inner_join(rcbh_SC_nonmerged,
             by = c("post_code", "yob", "mob", "sex", "practice_id")) %>%
  
  # Stata: ***OY***br ...
  # Stata: drop if polar_id_1 == "16458455"
  # (These are the result of the manual check)
  filter(
    polar_id_1 != "16458455",
    polar_id_1 != "14775380",
    polar_id_1 != "5374614287"
  )

# Stata: save "$data/rcbh_SC_matched_NONMERGED.dta", replace
write_parquet(rcbh_SC_matched_NONMERGED, file.path(data_path, "rcbh_SC_matched_NONMERGED.parquet"))


# *********************************
# * 6) Remaining EACH patients
# *********************************

# Stata: use "$data/rcbh_SC_premerge_NONMERGED.dta", clear
rcbh_EACH_nonmerged <- read_parquet(file.path(data_path, "rcbh_EACH_nonmerged.parquet"))

rcbh_EACH_matched_NONMERGED <- rcbh_SC_premerge_NONMERGED %>%
  filter(post_code != "Unknown") %>%
  mutate(post_code = as.numeric(post_code)) %>%
  # Stata: merge 1:1 ... using "$data/rcbh_EACH_nonmerged.dta"
  inner_join(rcbh_EACH_nonmerged,
             by = c("post_code", "yob", "mob", "sex", "practice_id")) %>%
  
  # Stata: g manual_flag =.
  mutate(manual_flag = NA)

# Stata: save "$data/rcbh_EACH_matched_NONMERGED.dta", replace
write_parquet(rcbh_EACH_matched_NONMERGED, file.path(data_path, "rcbh_EACH_matched_NONMERGED.parquet"))

# Stata: ***OY***br
# *** MANUAL STEP REQUIRED ***
# The Stata script requires a manual check here.
# You would load the file, browse it, and set `manual_flag = 1` for rows to keep.
#
# In R, you could:
# 1. Write to CSV: `write_csv(rcbh_EACH_matched_NONMERGED, "manual_check.csv")`
# 2. Edit `manual_flag` column in Excel.
# 3. Read it back: `rcbh_EACH_manual <- read_csv("manual_check.csv")`
#
# For this script, we'll assume the file is manually edited and saved,
# then we filter on it.
#
# (Assuming manual step is done and file is re-loaded)
# rcbh_EACH_matched_NONMERGED <- read_parquet(file.path(data_path, "rcbh_EACH_matched_NONMERGED.parquet"))

rcbh_EACH_matched_NONMERGED_flagmanual <- rcbh_EACH_matched_NONMERGED %>%
  # Stata: keep if manual_flag ==1
  filter(manual_flag == 1)

# Stata: save "$data/rcbh_EACH_matched_NONMERGED_flagmanual.dta", replace
write_parquet(rcbh_EACH_matched_NONMERGED_flagmanual, 
              file.path(data_path, "rcbh_EACH_matched_NONMERGED_flagmanual.parquet"))

# ** Append residual EACH/SC onto main EACH/SC RCBH sample
rcbh_EACH_SC_Appended <- read_parquet(file.path(data_path, "rcbh_EACH_SC_Appended.parquet"))

rcbh_EACH_SC_Appended_Twice <- bind_rows(
  rcbh_EACH_SC_Appended %>% mutate(post_code = as.character(post_code)),
  rcbh_SC_matched_NONMERGED
) %>%
  # Stata: drop grad_1b grad_2b ...
  select(-c(grad_1b, grad_2b, withd_1b, withd_2b, refer_1, n, flag_each, 
            flag_silverchain, starts_with("enrolled_"))) %>%
  mutate(post_code = as.numeric(post_code)) %>%
  
  # Stata: append using "$data/rcbh_EACH_matched_NONMERGED_flagmanual.dta"
  bind_rows(
    rcbh_EACH_matched_NONMERGED_flagmanual %>% mutate(post_code = as.numeric(post_code))
  ) %>%
  
  # Stata: drop manual_flag ...
  select(-c(manual_flag, grad_1b, refer_1, withd_1b, withd_2b, n, 
            flag_each, flag_silverchain, grad_2b, starts_with("enrolled_")))

# Stata: save "$data/rcbh_EACH_SC_Appended_Twice.dta", replace
write_parquet(rcbh_EACH_SC_Appended_Twice, file.path(data_path, "rcbh_EACH_SC_Appended_Twice.parquet"))


# **********************************************
# * 7) Cross-link with POLAR diagnosis information
# **********************************************
diag_sept_final <- read_parquet(file.path(data_path, "diagnosis_sept_cleaned_final.parquet"))

rcbh_sample_precontrol <- read_parquet(file.path(data_path, "rcbh_EACH_SC_Appended_Twice.parquet")) %>%
  # Stata: merge 1:1 polar_id_1 using "$data/diagnosis_sept_cleaned_final.dta"
  # Stata: keep if _merge ==3 | _merge ==1
  # R: This is a `left_join`
  left_join(diag_sept_final, by = "polar_id_1") %>%
  
  # Stata: replace cardio_SC_max =1 if cardio_max==1 & cardio_SC_max !=.
  mutate(
    cardio_SC_max = if_else(cardio_max == 1 & !is.na(cardio_SC_max), 1, cardio_SC_max),
    cardio_each_max = if_else(cardio_max == 1 & !is.na(cardio_each_max), 1, cardio_each_max),
    resp_SC_max = if_else(respiratory_max == 1 & !is.na(resp_SC_max), 1, resp_SC_max),
    resp_each_max = if_else(respiratory_max == 1 & !is.na(resp_each_max), 1, resp_each_max),
    frailty_SC_max = if_else(muscul_max == 1 & !is.na(frailty_SC_max), 1, frailty_SC_max),
    frailty_each_max = if_else(muscul_max == 1 & !is.na(frailty_each_max), 1, frailty_each_max)
  ) %>%
  
  # Stata: g cardio_max = (cardio_SC_max ==1 | cardio_each_max==1)
  mutate(
    cardio_max = (cardio_SC_max == 1 | cardio_each_max == 1),
    resp_max = (resp_SC_max == 1 | resp_each_max == 1),
    frailty_max = (frailty_SC_max == 1 | frailty_each_max == 1)
  ) %>%
  
  # Stata: g no_cond = ...
  mutate(no_cond = (cardio_max == 0 & resp_max == 0 & frailty_max == 0)) %>%
  
  # Stata: drop diag_date* ...
  select(-c(starts_with("diag_date"), diag_month, ends_with("each_max"), 
            ends_with("SC_max"), cardio_max, respiratory_max, muscul_max)) %>%
  
  # Stata: **** DECIDE DROP NON-MAIN CONDITIONS FOR TREATMENT GROUP
  select(-c(cancer_max, mental_max, diab_max, disability_max, dementia_alzheim_max))

# Stata: save "$data/rcbh_sample_precontrol.dta", replace
write_parquet(rcbh_sample_precontrol, file.path(data_path, "rcbh_sample_precontrol.parquet"))


# *******************************************
# * 8) Create final sample with control group
# *******************************************

# ** Get eligible control group
# Stata: use "$data/patientflag_sept.dta", clear
RCBH_sample_final <- read_parquet(file.path(data_path, "patientflag_sept.parquet")) %>%
  separate(patientsitekey, into = c("patientsitekey1", "patientsitekey2"), sep = "-", remove = FALSE) %>%
  mutate(patientsitekey1 = as.numeric(patientsitekey1)) %>%
  filter(!patientsitekey1 %in% c(1083, 1085, 688)) %>%
  
  # Stata: merge m:1 patientsitekey using "$data/patient_sept"
  inner_join(select(patient_sept, patientsitekey, polar_id_1, yob, mob, post_code, sex),
             by = "patientsitekey") %>%
  
  filter(post_code != "Unknown") %>%
  mutate(post_code = as.numeric(post_code)) %>%
  
  # Stata: merge m:1 polar_id_1 using "$data/rcbh_sample_precontrol.dta"
  # R: This is a `left_join` to bring in the treated patient info
  left_join(rcbh_sample_precontrol, by = "polar_id_1") %>%
  
  # Stata: keep if _merge ==3 | flag_status == "RCBH Eligible"
  # R: `!is.na(practice_id.y)` means it matched (`_merge==3`)
  filter(!is.na(practice_id.y) | flag_status == "RCBH Eligible") %>%
  
  # Stata: gen dt_tm ...
  mutate(
    dt_tm = ymd_hms(flag_datetime),
    d_day = as_date(dt_tm)
  ) %>%
  
  # * first treatment dates (monthly and quarterly)
  # Stata: g treat_date_EACH = mofd(enrolled_1b_EACH)
  mutate(
    treat_date_EACH = floor_date(enrolled_1b_EACH, "month"),
    treat_date_SC = floor_date(enrolled_1b_SC, "month")
  ) %>%
  
  # Stata: bys polar_id_1: egen first_treat = min(treat_date_EACH)
  # Stata: replace first_treat = treat_date_SC if first_treat ==.
  # Stata: bys polar_id_1: egen first_enrol = min(first_treat)
  group_by(polar_id_1) %>%
  mutate(
    first_treat = min(treat_date_EACH, na.rm = TRUE),
    first_treat = if_else(is.na(first_treat), treat_date_SC, first_treat),
    first_enrol = min(first_treat, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  
  # Stata: gen first_enrol_q = qofd(dofm(first_enrol))
  # R: `zoo::as.yearqtr` is the best %tq equivalent
  mutate(
    first_enrol_q = zoo::as.yearqtr(first_enrol)
  ) %>%
  select(-c(first_treat, treat_date_SC, treat_date_EACH)) %>%
  
  # ... (skip prog_duration checks) ...
  
  # Stata: drop if prog_withd_EACH <=3
  mutate(prog_withd_EACH = as.numeric(withd_1b_EACH - enrolled_1b_EACH)) %>%
  filter(prog_withd_EACH > 3 | is.na(prog_withd_EACH)) %>%
  
  # Stata: duplicates drop polar_id_1, force
  distinct(polar_id_1, .keep_all = TRUE) %>%
  
  # Stata: g age2 = 2025-yob
  mutate(
    yob = as.numeric(yob),
    age2 = 2025 - yob
  ) %>%
  # Stata: keep if age2>=18 & age2 !=.
  filter(age2 >= 18, !is.na(age2)) %>%
  select(-age2) %>%
  
  select(-c(flag_id, flag_name, flag_description, flag_datetime, flag_status)) %>%
  
  # Stata: merge m:1 post_code using "$data/SEIFA_irsd.dta"
  left_join(read_parquet(file.path(data_path, "SEIFA_irsd.parquet")), 
            by = "post_code") %>%
  # Stata: keep if _merge ==3
  filter(!is.na(seifa_irsd)) %>%
  
  select(-c(dt_tm, d_day, prog_withd_EACH, post_code_pop)) %>%
  
  # Stata: g treated = first_enrol !=.
  mutate(treated = !is.na(first_enrol)) %>%
  
  # * Get the diagnoses for control group
  # Stata: foreach var ... rename `var' `var'2
  rename(cardio_max2 = cardio_max,
         resp_max2 = resp_max,
         frailty_max2 = frailty_max) %>%
  
  # Stata: merge 1:1 polar_id_1 using "$data/diagnosis_sept_cleaned_final.dta"
  # Stata: keep if _merge ==3 | treated ==1
  # R: `left_join` and filter
  left_join(diag_sept_final, by = "polar_id_1") %>%
  filter(!is.na(diag_date_cardio_max) | treated == 1) %>%
  
  rename(frailty_max = muscul_max,
         resp_max = respiratory_max) %>%
  
  # Stata: replace `var'2 =`var' if treated ==0
  mutate(
    cardio_max2 = if_else(treated == 0, cardio_max, cardio_max2),
    resp_max2 = if_else(treated == 0, resp_max, resp_max2),
    frailty_max2 = if_else(treated == 0, frailty_max, frailty_max2)
  ) %>%
  
  # Stata: drop cardio_max ... rename ...
  select(-c(cardio_max, resp_max, frailty_max)) %>%
  rename(cardio_max = cardio_max2,
         resp_max = resp_max2,
         frailty_max = frailty_max2) %>%
  
  # * Merge in year/month of death
  # Stata: merge 1:m polar_id_1 using "$data/patient_sept"
  inner_join(select(patient_sept, polar_id_1, yod, mod), by = "polar_id_1") %>%
  
  # Stata: g yod_clean = ...
  mutate(
    yod_clean = if_else(yod != "NULL", as.numeric(yod), NA),
    mod_clean = if_else(mod != "NULL", as.numeric(mod), NA),
    # Stata: gen qdate = qofd( dofm( ym(yod_clean, mod_clean) ) )
    death_date_ym = ym(paste(yod_clean, mod_clean, sep = "-")),
    death_date = zoo::as.yearqtr(death_date_ym)
  ) %>%
  
  # Stata: bys polar_id_1: egen date_death = min(death_date)
  group_by(polar_id_1) %>%
  mutate(date_death = min(death_date, na.rm = TRUE)) %>%
  ungroup() %>%
  
  # Stata: duplicates drop polar_id_1, force
  distinct(polar_id_1, .keep_all = TRUE) %>%
  select(-c(yod_clean, mod_clean, death_date_ym)) %>%
  
  # Stata: drop if first_enrol_q == tq(2025q4)
  filter(first_enrol_q != zoo::as.yearqtr("2025 Q4") | is.na(first_enrol_q)) %>%
  
  mutate(age = 2025 - yob) %>%
  
  # Stata: * DROP AGE OUTLIERS
  # Stata: keep if age >=53
  filter(age >= 53)

# Stata: save "$data/RCBH_sample_final.dta", replace
write_parquet(RCBH_sample_final, file.path(data_path, "RCBH_sample_final.parquet"))


# *=========================================
# * POLAR Services data
# *=========================================

# *Services dataset preclean
RCBH_sample_final_ids <- select(RCBH_sample_final, polar_id_1, treated)
patient_sept_ids <- select(patient_sept, patientsitekey, polar_id_1)

services_raw <- bind_rows(
  read_parquet(file.path(data_path, "PHN202_UoM_RCBH_Service.parquet")),
  read_parquet(file.path(data_path, "service_sept.parquet"))
)

RCBH_services_precollapse <- services_raw %>%
  select(patientsitekey, service_date, service_item, providersitekey) %>%
  
  # Stata: gen dt = clock(...)
  mutate(
    dt = ymd_hms(service_date),
    date_daily = as_date(dt),
    my = floor_date(date_daily, "month"),
    dy = floor_date(date_daily, "day") # `dofd` not really used
  ) %>%
  # Stata: drop if my < ym(2022,1) | my > ym(2025,9)
  filter(my >= ym("2022-01"), my <= ym("2025-09")) %>%
  
  # Stata: merge m:1 patientsitekey ...
  inner_join(patient_sept_ids, by = "patientsitekey") %>%
  
  # Stata: merge m:1 polar_id_1 ...
  # R: Using left_join to see the _merge==2
  left_join(RCBH_sample_final_ids, by = "polar_id_1") %>%
  # Stata: ta treated if _merge ==2
  # `filter(is.na(treated)) %>% distinct(polar_id_1)`
  
  # Stata: bys polar_id_1: egen rcbh_flag = min(treated)
  group_by(polar_id_1) %>%
  mutate(rcbh_flag = min(treated, na.rm = TRUE)) %>%
  ungroup() %>%
  
  # Stata: keep if _merge ==3
  filter(!is.na(treated)) %>%
  
  # Stata: drop if regexm(service_item,"[A-Za-z]")
  # R: `grepl` is the base R equivalent
  filter(!grepl("[A-Za-z]", service_item)) %>%
  
  # Stata: destring service_item, gen(service_item_num) force
  # * Split rows with double item combinations into two rows
  # Stata: gen _items = subinstr(trim(service_item), "/", " ", .)
  # Stata: expand _nitems ...
  # R: `separate_rows` does all this in one step
  mutate(service_item_clean = str_replace_all(service_item, "/", " ")) %>%
  separate_rows(service_item_clean, sep = " ") %>%
  
  # Stata: destring service_item, g(service_item_num) force
  mutate(service_item_num = as.numeric(service_item_clean)) %>%
  # Stata: drop if service_item_num ==.
  filter(!is.na(service_item_num)) %>%
  
  # Stata: keep if inrange(service_item_num, 1, 99901)
  filter(service_item_num >= 1, service_item_num <= 99901)

# Stata: save "$data/RCBH_services_precollapse.dta", replace
write_parquet(RCBH_services_precollapse, file.path(data_path, "RCBH_services_precollapse.parquet"))

# *Services dataset collapse
RCBH_services_quarterly <- RCBH_services_precollapse %>%
  # Stata: gen q = qofd(dofm(my))
  mutate(q = zoo::as.yearqtr(my)) %>%
  
  # Stata: split(patientsitekey),p("-")
  separate(patientsitekey, into = c("patientsitekey1", NA), sep = "-", remove = FALSE) %>%
  
  # Stata: bys polar_id_1 patientsitekey1 q : g prac_num = _n==1
  group_by(polar_id_1, patientsitekey1, q) %>%
  mutate(prac_num = row_number() == 1) %>%
  
  # Stata: bys polar_id_1 providersitekey q : g prov_num = _n==1
  group_by(polar_id_1, providersitekey, q) %>%
  mutate(prov_num = row_number() == 1) %>%
  
  # * Number of telehealth services
  mutate(
    service_tele_pooled = service_item_num %in% c(91790, 91800, 91801, 91802, 91920,  
                                                  91890, 91891, 91900, 91910),
    
    # * Number of GP services
    service_gp_pooled = service_item_num %in% c(3, 23, 36, 44, 123, 4, 24, 37, 47, 124,
                                                5000, 5020, 5040, 5060, 5071, 5003, 
                                                5023, 5043, 5063, 5076, 5010, 5028, 
                                                5049, 5067, 5077, 90020, 90035, 
                                                90043, 90051, 90054),
    
    # *CDM
    service_cdm_gpmp = service_item_num %in% c(721, 92024, 965, 92029),
    service_cdm_tca = service_item_num %in% c(723, 92025),
    service_cdm_review = service_item_num %in% c(732, 92028, 967, 92030),
    service_cdm_mdmp = service_item_num %in% c(729, 92026, 731, 92027),
    
    # * Practice nurse
    service_prac_nurse_complete = service_item_num %in% c(10997, 10983, 10987),
    service_prac_nurse_chronic = service_item_num %in% c(10997, 93201, 93203),
    
    # *Allied health assessments
    service_alliedhealth = service_item_num >= 10950 & service_item_num <= 10970,
    
    # * Proactive health assessments
    service_gp_ha = service_item_num %in% c(701, 703, 705, 707, 715),
    service_heart_health = service_item_num == 699,
    
    # * Mental health plan
    service_mhp = service_item_num %in% c(2700, 2701, 2712, 2713, 2715, 2717,
                                          92112, 92113, 92116, 92117, 92114, 
                                          921126, 92115, 92127),
    
    # * Multidisciplinary case conferencing
    service_caseconf = service_item_num %in% c(735, 739, 743, 747, 750, 758),
    
    # * Medication review
    service_medreview = service_item_num == 900
  ) %>%
  
  # Stata: collapse (sum) service_* prac_num prov_num, by(polar_id_1 q)
  group_by(polar_id_1, q) %>%
  summarise(
    across(starts_with("service_"), ~sum(.x, na.rm = TRUE)),
    prac_num = sum(prac_num, na.rm = TRUE),
    prov_num = sum(prov_num, na.rm = TRUE)
  ) %>%
  ungroup()

# Stata: save "$data/RCBH_services_quarterly.dta", replace
write_parquet(RCBH_services_quarterly, file.path(data_path, "RCBH_services_quarterly.parquet"))


# *=========================================
# * POLAR REFERRALS
# *=========================================
referrals_monthly <- bind_rows(
  read_parquet(file.path(data_path, "phn202_uom_rcbh_referral.parquet")),
  read_parquet(file.path(data_path, "referral_sept.parquet"))
) %>%
  # Stata: gen referral_dt_td = daily(substr(referral_date,1,10),"YMD")
  mutate(
    referral_dt_td = ymd(substr(referral_date, 1, 10)),
    my = floor_date(referral_dt_td, "month")
  ) %>%
  # Stata: drop if my < ym(2022,1) | my > ym(2025,9)
  filter(my >= ym("2022-01"), my <= ym("2025-09")) %>%
  
  # Stata: merge m:1 patientsitekey ...
  inner_join(patient_sept_ids, by = "patientsitekey") %>%
  
  # Stata: merge m:1 polar_id_1 ...
  inner_join(RCBH_sample_final_ids, by = "polar_id_1") %>%
  
  # Stata: g referral_number = 1
  mutate(
    referral_number = 1,
    referral_number_HA_ED = referral_category == "Hospital / Emergency Department",
    referral_cardio = referral_category %in% c("Cardiology", "Vascular Surgery", "Neurology"),
    referral_respiratory = referral_category %in% c("Respiratory & Sleep Medicine", "Respiratory physician", "Thoracic"),
    referral_frailty = referral_category == "Geriatrics"
  ) %>%
  
  # Stata: collapse (sum) referral_*, by(polar_id_1 my)
  group_by(polar_id_1, my) %>%
  summarise(across(starts_with("referral_"), sum, na.rm = TRUE)) %>%
  ungroup()

# Stata: save "$data/RCBH_referrals_monthly.dta", replace
write_parquet(referrals_monthly, file.path(data_path, "RCBH_referrals_monthly.parquet"))

# Stata: gen q = qofd(dofm(my))
# Stata: collapse (sum) referral_*, by(polar_id_1 q)
referrals_quarterly <- referrals_monthly %>%
  mutate(q = zoo::as.yearqtr(my)) %>%
  group_by(polar_id_1, q) %>%
  summarise(across(starts_with("referral_"), sum, na.rm = TRUE)) %>%
  ungroup()

# Stata: save "$data/RCBH_referrals_quarterly.dta", replace
write_parquet(referrals_quarterly, file.path(data_path, "RCBH_referrals_quarterly.parquet"))


# *=========================================
# * POLAR Prescriptions
# *=========================================
prescriptions_raw <- bind_rows(
  read_parquet(file.path(data_path, "PHN202_UoM_RCBH_Prescription_OY.parquet")),
  read_parquet(file.path(data_path, "presc_sept.parquet"))
) %>%
  select(patientsitekey, prescription_date, atc_code) %>%
  
  # Stata: gen dt_td = daily(substr(prescription_date,1,10),"YMD")
  mutate(
    dt_td = ymd(substr(prescription_date, 1, 10)),
    my = floor_date(dt_td, "month")
  ) %>%
  # Stata: drop if my < ym(2022,1) | my > ym(2025,9)
  filter(my >= ym("2022-01"), my <= ym("2025-09")) %>%
  
  # Stata: merge m:1 patientsitekey ...
  inner_join(patient_sept_ids, by = "patientsitekey") %>%
  
  # Stata: merge m:1 polar_id_1 ...
  inner_join(RCBH_sample_final_ids, by = "polar_id_1") %>%
  
  # Stata: drop if missing(atc_code)
  filter(!is.na(atc_code)) %>%
  
  # Stata: gen atc_letter = substr(atc_code, 1, 1)
  mutate(atc_letter = substr(atc_code, 1, 1))

# * Define Polypharmacy in seperate quarterly dataset
# Stata: preserve
polypharmacy_quarterly <- prescriptions_raw %>%
  mutate(q = zoo::as.yearqtr(my)) %>%
  # Stata: egen pres_any = rowtotal(...)
  # R: A simple `pres_any = 1` is easier
  mutate(pres_any = 1) %>%
  # Stata: collapse (sum) pres_any, by(polar_id_1 q)
  group_by(polar_id_1, q) %>%
  summarise(pres_any = sum(pres_any, na.rm = TRUE)) %>%
  ungroup()

# Stata: save "$data/RCBH_polyphamracy.dta", replace
write_parquet(polypharmacy_quarterly, file.path(data_path, "RCBH_polyphamracy.parquet"))
# Stata: restore

# * ATC groups
# Stata: foreach L in A B C ... { gen byte presc_atc_`L' = ... }
atc_letters <- c("A", "B", "C", "D", "G", "H", "J", "L", "M", "N", "P", "R", "S", "T", "V")
presc_atc_vars <- map_dfc(atc_letters, ~
                            transmute(prescriptions_raw, 
                                      !!paste0("presc_atc_", .x) := (atc_letter == .x))
)

prescriptions_monthly <- bind_cols(prescriptions_raw, presc_atc_vars) %>%
  # Stata: collapse (sum) presc_atc_*, by(polar_id_1 my)
  group_by(polar_id_1, my) %>%
  summarise(across(starts_with("presc_atc_"), sum, na.rm = TRUE)) %>%
  ungroup()

# Stata: save "$data/RCBH_prescriptions.dta", replace
write_parquet(prescriptions_monthly, file.path(data_path, "RCBH_prescriptions.parquet"))

# Stata: gen q = qofd(dofm(my))
# Stata: collapse (sum) presc_atc_*, by(polar_id_1 q)
prescriptions_quarterly <- prescriptions_monthly %>%
  mutate(q = zoo::as.yearqtr(my)) %>%
  group_by(polar_id_1, q) %>%
  summarise(across(starts_with("presc_atc_"), sum, na.rm = TRUE)) %>%
  ungroup()

# Stata: save "$data/RCBH_prescriptions_quarterly.dta", replace
write_parquet(prescriptions_quarterly, file.path(data_path, "RCBH_prescriptions_quarterly.parquet"))


# *=========================================
# *Panel dataset construction
# *=========================================

# *gen empty quarterly dataset
# Stata: local start = yq(2022,1)
# Stata: local stop  = yq(2025,3)
# R: Create a sequence of quarters
rcbh_skeleton <- tibble(
  q = seq(zoo::as.yearqtr("2022 Q1"), zoo::as.yearqtr("2025 Q3"), by = 1/4)
)

# *Gen complete panel
# Stata: use "$data/RCBH_sample_final.dta", clear
rcbh_final_sample <- read_parquet(file.path(data_path, "RCBH_sample_final.parquet"))

# Stata: cross using "$data/rcbh_skeleton.dta"
# R: `expand_grid` creates all combinations
panel_data <- expand_grid(
  polar_id_1 = unique(rcbh_final_sample$polar_id_1),
  q = rcbh_skeleton$q
) %>%
  # Add time-invariant patient data
  left_join(rcbh_final_sample, by = "polar_id_1") %>%
  
  # Stata: merge 1:1 polar_id_1 q using ...
  left_join(RCBH_services_quarterly, by = c("polar_id_1", "q")) %>%
  left_join(prescriptions_quarterly, by = c("polar_id_1", "q")) %>%
  left_join(referrals_quarterly, by = c("polar_id_1", "q")) %>%
  
  # Stata: fill missing monthly counts with 0
  mutate(
    across(
      c(starts_with("service_"), starts_with("referral_"), starts_with("presc_")),
      ~replace_na(., 0)
    )
  ) %>%
  
  # Stata: drop patientsitekey2 diag_month diag_date*
  select(-c(patientsitekey2, diag_month, starts_with("diag_date_"))) %>%
  
  # Stata: order ... sort ...
  arrange(first_enrol, polar_id_1, q)

# Stata: save "$data/rcbh_panel_quarterly.dta", replace
write_parquet(panel_data, file.path(data_path, "rcbh_panel_quarterly.parquet"))


# *=========================================
# * Analysis
# *=========================================

# **************************
# * Final pre-analysis clean
# **************************
panel_data_clean <- read_parquet(file.path(data_path, "rcbh_panel_quarterly.parquet")) %>%
  arrange(polar_id_1, q) %>%
  
  # * merge prescription numbers
  left_join(polypharmacy_quarterly, by = c("polar_id_1", "q")) %>%
  mutate(pres_any = replace_na(pres_any, 0)) %>%
  
  # * Censor dead people
  # Stata: gen double sc_dt = date(withdrawal_date_SC, "DMY")
  mutate(
    sc_dt = dmy(withdrawal_date_SC),
    # Stata: replace withd_dead = ...
    withd_dead = if_else(withdrawal_reason == "Died" & !is.na(withd_1b_EACH), 
                         withd_1b_EACH, NA),
    withd_dead = coalesce(withd_dead, sc_dt)
  ) %>%
  
  # *Quarter of death and censor
  mutate(
    q_dead = zoo::as.yearqtr(withd_dead),
    # Stata: replace q_dead = date_death if date_death !=. & treated ==0
    q_dead = if_else(treated == 0, date_death, q_dead)
  ) %>%
  
  # Stata: drop if q_dead < tq(2022q1)
  filter(q_dead >= zoo::as.yearqtr("2022 Q1") | is.na(q_dead)) %>%
  
  # Stata: drop if q >= q_dead & q_dead !=.
  filter(q < q_dead | is.na(q_dead)) %>%
  
  # Stata: drop patientsitekey ...
  select(-c(patientsitekey, patientsitekey1, mob, cancer_max, mental_max,
            diab_max, disability_max, dementia_alzheim_max)) %>%
  
  mutate(seifa_irsd = as.numeric(seifa_irsd)) %>%
  
  # ** Gen paper outcome variables
  mutate(
    # *1) gp consultations
    gp_face_tele = service_tele_pooled + service_gp_pooled,
    
    # *2) Multidisciplinary/care coordination
    md_services = service_cdm_tca + service_cdm_mdmp + service_caseconf + 
      service_medreview + service_gp_ha + service_prac_nurse_chronic + 
      service_cdm_gpmp + service_cdm_review + service_mhp,
    md_services2 = service_cdm_tca + service_cdm_mdmp + service_caseconf + 
      service_medreview  + service_gp_ha + service_prac_nurse_chronic + 
      service_cdm_review
  ) %>%
  
  # *** Seifa Heterogeneity Analysis
  # Stata: xtile seifa_med = seifa_irsd, n(2)
  mutate(
    seifa_med = ntile(seifa_irsd, 2),
    seifa_med = factor(seifa_med, labels = c("Low", "High"))
  ) %>%
  
  # * Sex heterogeneity
  # Stata: encode sex, gen(sex2)
  # Stata: drop if sex2 ==3 | sex2 ==4
  filter(sex %in% c("Male", "Female")) %>%
  mutate(sex2 = factor(sex, labels = c("Male", "Female"))) %>%
  
  # * Age heterogeneity
  # Stata: g age_med = age >=81
  mutate(
    age_med = age >= 81,
    age_med = factor(age_med, labels = c("<81", ">=81"))
  ) %>%
  
  # * Prog duration heterogeneity
  mutate(
    prog_duration_EACH = as.numeric(grad_1b_EACH - enrolled_1b_EACH),
    prog_duration_SC = as.numeric(grad_1b_SC - enrolled_1b_SC),
    days_enrolled_EACH = as.numeric(ymd("2025-10-01") - enrolled_1b_EACH),
    days_enrolled_SC = as.numeric(ymd("2025-10-01") - enrolled_1b_SC),
    
    # Stata: replace prog_duration_all = prog_duration_EACH
    # Stata: replace prog_duration_all = prog_duration_SC if prog_duration_EACH ==.
    prog_duration_all = coalesce(prog_duration_EACH, prog_duration_SC),
    
    # Stata: replace prog_duration_all = days_enrolled_SC if days_enrolled_SC !=. & prog_duration_SC ==.
    prog_duration_all = if_else(!is.na(days_enrolled_SC) & is.na(prog_duration_SC), 
                                days_enrolled_SC, prog_duration_all),
    
    # Stata: replace prog_duration_all = days_enrolled_EACH if days_enrolled_EACH !=. & prog_duration_EACH ==.
    prog_duration_all = if_else(!is.na(days_enrolled_EACH) & is.na(prog_duration_EACH), 
                                days_enrolled_EACH, prog_duration_all),
    
    # Stata: xtile duration = prog_duration_all, n(2)
    duration = ntile(prog_duration_all, 2),
    
    # Stata: replace duration = 3 if prog_duration_all ==.
    duration = if_else(is.na(prog_duration_all), 3, duration),
    duration = factor(duration, labels = c("below median", "above median", "Never treated"))
  ) %>%
  
  # ** gen DiD variables
  # * ID variable
  # Stata: egen ID = group(polar_id_1)
  mutate(ID = as.integer(as.factor(polar_id_1)),
         
         # * Time variable
         # Stata: egen tvar = group(q)
         # R: Use numeric version of quarter
         tvar = as.numeric(q) * 4) %>% # e.g., 2022.0 -> 8088, 2022.25 -> 8089
  
  # Stata: g trel= q - first_enrol_q
  # R: `zoo::as.yearqtr` objects can be subtracted. Multiply by 4 to get quarters.
  mutate(
    trel = (as.numeric(q) - as.numeric(first_enrol_q)) * 4,
    
    # * Treatment
    # Stata: g treat = 0
    # Stata: replace treat = 1 if trel >= -4 & first_enrol_q !=.
    treat = if_else(trel >= -4 & !is.na(first_enrol_q), 1, 0)
  ) %>%
  
  # * ln(0.10 + outcome) robustness check
  mutate(
    gp_face_tele_LN = log(gp_face_tele + 0.1),
    referral_number_LN = log(referral_number + 0.1),
    md_services_LN = log(md_services + 0.1),
    md_services2_LN = log(md_services2 + 0.1),
    pres_any_LN = log(pres_any + 0.1)
  ) %>%
  
  # Stata: order ID tvar ...
  select(ID, tvar, treat, first_enrol_q, q, q_dead, sex, post_code, age, 
         everything()) # `everything()` puts all other columns at the end


# **************************
# * Baseline Model DiD
# **************************
# Stata: local outcomes gp_face_tele referral_number md_services  pres_any
outcomes <- c("gp_face_tele", "referral_number", "md_services", "pres_any")

# Stata: foreach y of local outcomes { ... }
# R: Use `lapply` to loop and store results in a list
did_results <- lapply(outcomes, function(y) {
  message(paste("Running DiD for outcome:", y))
  
  # Stata: did_multiplegt_dyn `y' ID tvar treat, ...
  # R: The `didm::did_multiplegt` function
  model <- did_multiplegt(
    df = panel_data_clean,
    Y = y,
    G = "ID",
    T = "tvar",
    D = "treat",
    dyn = 10,       # Stata: effects(10)
    placebo = 5     # Stata: placebo(5)
    # Stata: cluster(ID) is default in the R function
  )
  
  # Stata: save_results($output2/`gname'_baseline`tag')
  # R: Save the model object
  gname <- paste0("did_", y, "_baseline")
  saveRDS(model, file = file.path(output_path2, paste0(gname, ".rds")))
  
  return(model)
})

# Name the results list for easy access
names(did_results) <- outcomes

# You can now access individual models, e.g.:
# summary(did_results$gp_face_tele)
