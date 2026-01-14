********************************************************************************
********************************************************************************
clear all
capture log close
set more off
set linesize 80
set matsize 800
set maxvar 10000
macro drop _all

/*global data "C:/Users/fisherrf/The University of Melbourne/Karinna Saxby - polar/data" // old data directory*/
 
glo data "C:\Users\fisherrf\The University of Melbourne\Karinna Saxby - polar\data\data_updated" // updated data directory september
glo output "C:\Users\fisherrf\The University of Melbourne\Karinna Saxby - polar\output\rcbh\matched" // results for CEM sample
glo output2 "C:\Users\fisherrf\The University of Melbourne\Karinna Saxby - polar\output\rcbh\unmatched" // results for baseline sample

********************************************************************************
********************************************************************************

*import polar excel files and save as .dta
local files: dir "$data" files "*.csv"

foreach f of local files {
    di "Processing file: `f'"
    
    import delimited "${data}/`f'", bindquote(strict) clear
    
    local fname = subinstr("`f'", ".csv", "", .)
    
    save "${data}/`fname'.dta", replace
}

*import september ONLY POLAR files and save as .dta 
foreach fn in presc_sept service_sept patient_sept patientflag_sept referral_sept diagnosis_sept {
    import delimited using "$data/`fn'.csv", bindquote(strict) clear
    save "$data/`fn'.dta", replace
}


               /************************************************
			    Import EMPHN monthly patient datasets & ABS Data
			   ************************************************/
**EACH
import excel "$data/EACH_final", firstrow sheet(Patient) clear // includes patient duplicates; drop them later
save "$data/monthly_EACH.dta", replace
import excel "$data/EACH_final", firstrow sheet(Patient_Conditions) clear // diagnosis dataset
save "$data/monthly_diagnosis_EACH.dta", replace

**Updated EACH dataset with date of birth variable 
*/ note1: They gave us a new dataset by special request with date of birth (DOB) and referral dates upto 30 september 2025,  but no enrolment date in this dataset*/
*/ note2: Merge in enrolment dates from monthly September EACH dataset that's without DOB 
import excel "$data/EACH_jul_DOB", firstrow clear // note: I called this dataset "july" because thats what they called it; it actually has referral dates upto september. 
drop EpisodeNo
rename PSK PatientSiteKey
duplicates drop PatientSiteKey, force 
merge 1:m PatientSiteKey using "$data/monthly_EACH.dta" // merge in monthly september dataset for enrolment dates
keep if _merge ==3 // drops 4 invalid obs
drop _merge 
keep Practice_ID PatientSiteKey Dateofbirth Sex Postcode ReferralReceivedDate Enrolled_Date Withdrawal_Date Graduated_Date Withdrawal_Reason
save "$data/monthly_EACH_DoB.dta", replace 

**Silverchain
import excel "$data/SC_final", firstrow sheet(PatientDetails) clear // september 2025 updated
save "$data/monthly_silverchain.dta", replace
import excel "$data/SC_final", firstrow sheet(PatientConditions) clear
save "$data/monthly_diagnosis_silverchain.dta", replace

* POLAR diagnosis dataset 
use "$data/diagnosis_sept.dta", clear 

g diag_dt = clock(diagnosis_recorded_date, "YMDhms")
format diag_dt %tc

g diag_date = dofc(diag_dt)
format diag_date %td
g diag_month = mofd(diag_date)
format diag_month %tm

keep if diag_month >= tm(2022m1) & diag_month <=tm(2025m9) // restrict to study sample diagnoses
keep patientsitekey chronic_disease_category diag_month

compress
save "$data/diagnosis_sept_cleaned.dta", replace 

* SEIFA
import excel "$data/SEIFA", firstrow sheet(Table 2) clear
rename AustralianBureauof post_code
rename B post_code_pop
rename C seifa_irsd
drop in 1
keep post_code post_code_pop seifa_irsd
destring post_code, force replace 
keep if inrange(post_code, 3000, 3999)
save "$data/SEIFA_irsd.dta", replace

	   
/**********************************************************
* 1) Clean the monthly datasets prior to merging with POLAR
***********************************************************/

        **************
        * EACH dataset
        **************
use "$data/monthly_EACH_DOB.dta", clear

rename Practice_ID practice_id 
rename Dateofbirth dob
rename Sex sex
rename Postcode post_code 
rename ReferralReceivedDate referral_date_EACH 
rename Enrolled_Date enrolled_date_EACH
rename Withdrawal_Date withdrawal_date_EACH 
rename Graduated_Date graduated_date_EACH 
rename Withdrawal_Reason withdrawal_reason

replace practice_id = "392" if practice_id == "392B" | practice_id == "392C"

* Generate unique variables for each persons multiple enrolment, withdrawal, and graduation dates (if exist)
duplicates drop PatientSiteKey enrolled_date_EACH, force // note: ok to drop like this; date maxed across panel

bys PatientSiteKey (enrolled_date_EACH): g n_enrol = _n
bys PatientSiteKey (graduated_date_EACH): g n_grad = _n
bys PatientSiteKey (withdrawal_date_EACH): g n_withd = _n

bys PatientSiteKey: g enrolled_1 = enrolled_date_EACH if n_enrol ==1
bys PatientSiteKey: egen enrolled_1b_EACH = min(enrolled_1)
format enrolled_1b %td
bys PatientSiteKey: g enrolled_2 = enrolled_date_EACH if n_enrol ==2
bys PatientSiteKey: egen enrolled_2b_EACH = min(enrolled_2)
format enrolled_2b %td
bys PatientSiteKey: g enrolled_3 = enrolled_date_EACH if n_enrol ==3
bys PatientSiteKey: egen enrolled_3b_EACH = min(enrolled_3)
format enrolled_3b %td

bys PatientSiteKey: g grad_1 = graduated_date_EACH if n_grad ==1
bys PatientSiteKey: egen grad_1b_EACH = min(grad_1)
format grad_1b %td
bys PatientSiteKey: g grad_2 = graduated_date_EACH if n_grad ==2
bys PatientSiteKey: egen grad_2b_EACH = min(grad_2)
format grad_2b %td
bys PatientSiteKey: g grad_3 = graduated_date_EACH if n_grad ==3
bys PatientSiteKey: egen grad_3b_EACH = min(grad_3)
format grad_3b %td

bys PatientSiteKey: g withd_1 = withdrawal_date_EACH if n_withd==1
bys PatientSiteKey: egen withd_1b_EACH = min(withd_1)
format withd_1b_EACH %td
bys PatientSiteKey: g withd_2 = withdrawal_date_EACH if n_withd==2
bys PatientSiteKey: egen withd_2b_EACH = min(withd_2)
format withd_2b_EACH %td
bys PatientSiteKey: g withd_3 = withdrawal_date_EACH if n_withd==3
bys PatientSiteKey: egen withd_3b_EACH = min(withd_3)
format withd_3b_EACH %td

bys PatientSiteKey: keep if _n==1

drop  enrolled_date_EACH withdrawal_date_EACH graduated_date_EACH n_enrol n_grad ///
 n_withd enrolled_1 enrolled_2 enrolled_3 grad_1 grad_2 grad_3 withd_1 withd_2 withd_3 
* Deal with new date of birth variable; dob is a STRING like "11nov1965 00:00:00"
**format into "yob" and "mob" variables for POLAR merge later
gen double dob_date = dofc(dob)
format dob_date %td
gen str4 yob = string(year(dob_date))
gen str2 mob = string(month(dob_date))             

* Merge diagnosis data in from monthly patient datasets
merge 1:m PatientSiteKey using "$data/monthly_diagnosis_EACH.dta", keepusing(Diagnosis)
unique(PatientSiteKey) if _merge ==1 // 133 patients have no diagnosis in EACH dataset; will have in POLAR maybe
g diag_miss_EACH = _merge ==1
drop _merge

g cardio_each = inlist(Diagnosis, "Chronic Complex- Angina", "Chronic Complex- Cardiovascular (heart) conditions", ///
"Chronic Complex- Cholesterol (lipid metabolism disorder)", "Chronic Complex- Chronic (congestive) heart failure", ///
"Chronic Complex- Hypertension")
bys PatientSiteKey: egen cardio_each_max = max(cardio_each)

g respiratory_each = inlist(Diagnosis, "Chronic Complex- Asthma", "Chronic Complex- Chronic obstructive pulmonary disease (COPD)", ///
"Chronic Complex- Respiratory conditions, other")
bys PatientSiteKey: egen resp_each_max = max(respiratory_each)

g frailty_each = inlist(Diagnosis, "Chronic Complex- Musculo-skeletal conditions, other", "Chronic Complex- Osteoarthritis", ///
"Chronic Complex- Osteoporosis", "Chronic Complex- Rheumatoid arthritis")

bys PatientSiteKey: egen frailty_each_max = max(frailty_each)

bys PatientSiteKey: keep if _n==1

bys post_code sex yob mob practice_id: gen N=_N
drop if N>1 // 14/706 
drop N

drop PatientSiteKey dob_date dob Diagnosis cardio_each respiratory_each frailty_each

save "$data/EACH_cleaned.dta", replace 

       *********************
       * Silverchain dataset
       *********************
use "$data/monthly_silverchain.dta", clear
split(PatientSiteKey), p("-")

keep PatientID PatientSiteKey PracticeName DOB Sex PostCode ReferralDate EnrolledDate WithdrawalDate GraduatedDate PatientSiteKey1

rename PatientSiteKey1 practice_id
rename Sex sex
rename PostCode post_code 
rename ReferralDate referral_date_SC
rename EnrolledDate enrolled_date_SC
rename WithdrawalDate withdrawal_date_SC 
rename GraduatedDate grad_1b_SC

bys PatientID (enrolled_date_SC): g n_enrol = _n

bys PatientID: g enrolled_1 = enrolled_date_SC if n_enrol ==1
bys PatientID: egen enrolled_1b_SC = min(enrolled_1)
format enrolled_1b_SC %td

bys PatientID: keep if _n==1 //only 3 SC patients with > 1 enrolment we just take first date no need to create more enrol vars
drop  enrolled_date_SC n_enrol enrolled_1

merge 1:m PatientID using "$data/monthly_diagnosis_silverchain.dta" // 7 with no diagnosis; leave as may be in POLAR

g cardio_SC = inlist(DiagnosisGroup, "Cardiovascular Disease")
bys PatientID: egen cardio_SC_max = max(cardio_SC)

g resp_SC = inlist(DiagnosisGroup, "Respiratory Disease")
bys PatientID: egen resp_SC_max = max(resp_SC)

g frailty_SC = inlist(DiagnosisGroup, "Frailty/Falls Risk")
bys PatientID: egen frailty_SC_max = max(frailty_SC)

drop if _merge ==2 // 1 empty row
drop _merge 

bys PatientID: keep if _n==1

* Gen the dates var as follows to match to POLAR later 
gen str4 yob = string(year(DOB))
gen str2 mob = string(month(DOB))

* Drop people
bys post_code sex yob mob practice_id: gen N=_N
keep if N ==1 // only 6 dropped!!!!!

keep sex post_code withdrawal_date_SC referral_date_SC grad_1b_SC practice_id enrolled_1b_SC cardio_SC_max resp_SC_max frailty_SC_max yob mob 

save "$data/silverchain_cleaned.dta", replace 


/*******************************************************************
* 2) Merge unique patients from EACH/Silverchain datasets to POLAR
*******************************************************************/

******************************
* Clean POLAR diagnosis data 
******************************										  
use "$data/diagnosis_sept_cleaned.dta", clear
compress 
merge m:1 patientsitekey using "$data/patient_sept", keepusing(polar_id_1)
keep if _merge ==3
drop _merge 

encode chronic_disease_category, g(sad)

g cardio = 1 if chronic_disease_category == "Cardiovascular"
g respiratory = 1 if chronic_disease_category == "Respiratory"
g cancer = 1 if chronic_disease_category == "Cancer"
g muscul = 1 if chronic_disease_category == "Musculoskeletal"
g mental = 1 if chronic_disease_category == "Mental Health"
g diab = 1 if chronic_disease_category == "Diabetes"
g disability = 1 if chronic_disease_category == "Disability"
g dementia_alzheim =1 if sad ==1
g chronic_other = 1 if chronic_disease_category == "AoD"
replace chronic_other = 1 if chronic_disease_category == "CKD"
replace chronic_other = 1 if chronic_disease_category == "Oral"

bys polar_id_1: egen cardio_max = max(cardio)
bys polar_id_1: egen respiratory_max = max(respiratory)
bys polar_id_1: egen cancer_max = max(cancer)
bys polar_id_1: egen muscul_max = max(muscul)
bys polar_id_1: egen mental_max = max(mental)
bys polar_id_1: egen diab_max = max(diab)
bys polar_id_1: egen disability_max = max(disability)
bys polar_id_1: egen other_max = max(chronic_other)
bys polar_id_1: egen dementia_alzheim_max = max(dementia_alzheim)

foreach var of varlist *_max {
	replace `var' = 0 if `var' ==.
}

g diag_date_cardio = diag_month if cardio==1
g diag_date_resp = diag_month if respiratory==1
g diag_date_cancer = diag_month if cancer==1
g diag_date_muscul = diag_month if muscul==1
g diag_date_mental = diag_month if mental==1
g diag_date_diab = diag_month if diab==1
g diag_date_disability = diag_month if disability==1
g diag_date_dementia_alz = diag_month if dementia_alzheim==1

foreach var of varlist diag_date_* {
	bys polar_id_1: egen `var'_max = min(`var') // extends diag date across panel for each condition
}

egen chronic_eligible = rowtotal(cardio_max respiratory_max muscul_max)
bys polar_id_1: gen chronic_dummy = chronic_eligible >0
keep if chronic_eligible >0 // people with at least one of CVD, CRD, frailty

duplicates drop polar_id_1, force

drop chronic_eligible dementia_alzheim disability diab mental muscul cancer respiratory cardio sad chronic_disease_category chronic_dummy other_max chronic_other diag_date_dementia_alz diag_date_disability diag_date_diab diag_date_mental diag_date_muscul diag_date_cancer diag_date_resp diag_date_cardio

format diag_date_* %tm

compress
save "$data/diagnosis_sept_cleaned_final.dta", replace // polar merged to diagnosis and vars defined 


************************************************************************
* 3) Clean POLAR patientflag (RCBH enrolled only) and merge in diagnosis
************************************************************************

** not in POLAR 
use "$data/patientflag_sept.dta", clear
keep if inlist(flag_status, "RCBH Enrolled", "RCBH Graduated", "RCBH Withdrawn")
split(patientsitekey),p("-")
destring patientsitekey1, replace

* Drop retrospective flagging patients that we have no enrolment date for because they flagged incorrectly
drop if inlist(patientsitekey1, 1085, 1083, 688) // note1: 1085 = colcester family medical in CRM, but = 409 in EACH provider data/
                                           // note2: 688 = mount evelyn medical clinical & lilydale doctors in CRM (we have to drop because POLAR 688 is
										   //        retrospective flagging only; doesnt matter anyway as only 2 patients in provider data with practice id 688)
										   // note3: no idea what practice 1083 is; not in latest provider data or CRM; dropping
										   	
g flag_each_prac = inlist(patientsitekey1, 82, 88, 103, 115, 125, 393, 510, 1066, 2028, ///
                    2029, 2030, 2031, 392)
g flag_sc_prac = inlist(patientsitekey1, 1445, 1797, 244, 247, 472, 59, 782, 820, 953, /// 
                            141, 977, 2154, 847, 970, 1001, 1523, 968)

* keep people ever enrolled 
** note: Later merge remaining umerged back in to people that never enrolled but graduated or withdrew
g enrolled = (flag_status == "RCBH Enrolled")
bys patientsitekey: egen max_enrol = max(enrolled)
keep if max_enrol ==1 

* Get unique variables for each unique date of enrolment, graduated, withdrawal
gen dt_tm  = clock(flag_datetime, "YMDhms")
gen    d_day  = dofc(dt_tm)
format d_day %td

gen status = lower(word(flag_status, 2))  
bys patientsitekey status (dt): gen seq = _n

g enrolled_1 = d_day if seq ==1 & status =="enrolled"
format enrolled_1 %td
g enrolled_2 = d_day if seq ==2 & status =="enrolled"
format enrolled_2 %td
g enrolled_3 = d_day if seq ==3 & status =="enrolled"
format enrolled_3 %td
g enrolled_4 = d_day if seq ==4 & status =="enrolled"
format enrolled_4 %td
bys patientsitekey: egen enrolled_1b = min(enrolled_1)
format enrolled_1b %td
bys patientsitekey: egen enrolled_2b = min(enrolled_2)
format enrolled_2b %td
bys patientsitekey: egen enrolled_3b = min(enrolled_3)
format enrolled_3b %td
bys patientsitekey: egen enrolled_4b = min(enrolled_4)
format enrolled_4b %td

g grad_1 = d_day if seq ==1 & status =="graduated"
format grad_1 %td
g grad_2 = d_day if seq ==2 & status =="graduated"
format grad_2 %td
bys patientsitekey: egen grad_1b = min(grad_1)
format grad_1b %td
bys patientsitekey: egen grad_2b = min(grad_2)
format grad_2b %td

g withd_1 = d_day if seq ==1 & status =="withdrawn"
format withd_1 %td
g withd_2 = d_day if seq ==2 & status =="withdrawn"
format withd_2 %td
bys patientsitekey: egen withd_1b = min(withd_1)
format withd_1b %td
bys patientsitekey: egen withd_2b = min(withd_2)
format withd_2b %td

drop enrolled_1 enrolled_2 enrolled_3 enrolled_4 grad_1 grad_2 withd_1 withd_2
bys patientsitekey: keep if _n ==1 

* Merge in POLAR id
merge 1:1 patientsitekey using "$data/patient_sept.dta", keepusing(polar_id_1 yob mob sex post_code practice_id)
keep if _merge ==3 // everyone merged
drop _merge

drop if patientsitekey == "782-24012" // 1 patient enrolled at 2 practices in POLAR; drop their first enrolment (1 day before second)

drop if yob == "2017" // 1 person

drop flag_id flag_name flag_description flag_datetime flag_status patientsitekey1 patientsitekey2 ///
flag_each_prac flag_sc_prac enrolled max_enrol dt_tm d_day status seq 

order polar_id_1 

save "$data/premerge_NoDropNYet.dta", replace


********************************
* 4) Merge SC/EACH data to POLAR
********************************
use "$data/premerge_NoDropNYet.dta", clear

* Get premerge dataset; just one is fine as EACH/SC both have yob/mob now 
preserve
bys post_code yob mob sex practice_id: g N = _N
keep if N ==1 // 20 deleted 
drop N
order polar_id_1 patientsitekey practice_id post_code sex yob mob
sort polar_id_1
tostring post_code, replace
tostring practice_id, replace
save "$data/rcbh_premerge.dta", replace
restore

* Now merge silverchain first 
use "$data/rcbh_premerge.dta", clear 
merge 1:1 post_code yob mob sex practice_id using "$data/silverchain_cleaned.dta" 
preserve 
keep if _merge ==2 // silverchain patients that didnt merge into this subset of polar of ever-enrolled
drop _merge
save "$data/rcbh_SC_nonmerged.dta", replace 
restore
keep if _merge ==3 // // 350 merged
 
drop enrolled_1b enrolled_2b enrolled_3b enrolled_4b grad_1b grad_2b withd_1b withd_2b _merge
save "$data/rcbh_SC_matched.dta", replace

*Then EACH
use "$data/rcbh_premerge.dta", clear 
destring post_code, replace
merge 1:1 post_code yob mob sex practice_id using "$data/EACH_cleaned.dta" // 440 matched!
preserve 
keep if _merge ==2 // now EACH patients that didnt merge into this subset of polar of ever_enrolled
drop _merge 
save "$data/rcbh_EACH_nonmerged.dta", replace
restore
keep if _merge ==3 // 440 merged

drop _merge enrolled_1b enrolled_2b enrolled_3b enrolled_4b grad_1b grad_2b withd_1b withd_2b 
save "$data/rcbh_EACH_matched.dta", replace

** Append the EACH and Silverchain datasets together 
use  "$data/rcbh_SC_matched.dta", clear 
destring post_code, replace 
append using "$data/rcbh_EACH_matched.dta"

save "$data/rcbh_EACH_SC_Appended.dta", replace // 790 patients total


***********************************
* 5) Remaining Silverchain patients
***********************************
** just copy paste cleaning code from above for now; although there is an easier way to do this 
use "$data/patientflag_sept.dta", clear

keep if inlist(flag_status, "RCBH Graduated", "RCBH Withdrawn", "RCBH  Referred", "RCBH Referred") // theres dates for all of these inside silverchain monthly dataset
split(patientsitekey),p("-")
destring patientsitekey1, replace

drop if inlist(patientsitekey1, 1083, 1085, 688) 
				
g flag_each = inlist(patientsitekey1, 82, 88, 103, 115, 125, 393, 510, 1066, 2028, ///
                    2029, 2030, 2031, 392)
g flag_silverchain = inlist(patientsitekey1, 1445, 1797, 244, 247, 472, 59, 782, 820, 953, /// 
                            141, 977, 2154, 847, 970, 1001, 1523, 968)
													
* Get unique variables for each unique date of enrolment, graduated, withdrawal
gen dt_tm  = clock(flag_datetime, "YMDhms")
gen    d_day  = dofc(dt_tm)
format d_day %td

gen status = lower(word(flag_status, 2))  
bys patientsitekey status (dt): gen seq = _n

g grad_1 = d_day if seq ==1 & status =="graduated"
format grad_1 %td
g grad_2 = d_day if seq ==2 & status =="graduated"
format grad_2 %td
bys patientsitekey: egen grad_1b = min(grad_1)
format grad_1b %td
bys patientsitekey: egen grad_2b = min(grad_2)
format grad_2b %td

g withd_1 = d_day if seq ==1 & status =="withdrawn"
format withd_1 %td
g withd_2 = d_day if seq ==2 & status =="withdrawn"
format withd_2 %td
bys patientsitekey: egen withd_1b = min(withd_1)
format withd_1b %td
bys patientsitekey: egen withd_2b = min(withd_2)
format withd_2b %td

g refer_1 = d_day if seq ==1 & status == "referred"
format refer_1 %td

bys patientsitekey: keep if _n ==1 // 1892 "patientsitekeys" remaining

* Merge in POLAR id
merge 1:1 patientsitekey using "$data/patient_sept.dta", keepusing(polar_id_1 yob mob sex post_code practice_id)
keep if _merge ==3 // everyone merged
drop _merge

destring yob, replace force 
g age = 2025 - yob
drop if age <18 // no age cannot merge 
drop if age ==. // no age cannot merge 
drop age

drop grad_1 grad_2 withd_1 withd_2 

save "$data/NoDropNYet_NONMERGED.dta", replace

** Get premerge datasets; do silverchain first (unique based on matching characteristics)
use "$data/NoDropNYet_NONMERGED.dta", clear
bys post_code yob mob sex practice_id: g N = _N
keep if N ==1 // 108 deleted 
drop flag_id flag_name flag_description flag_datetime flag_status patientsitekey1 patientsitekey2 dt_tm d_day status seq 
order polar_id_1 patientsitekey practice_id post_code sex N 
sort polar_id_1
tostring post_code, replace
tostring practice_id, replace
save "$data/rcbh_SC_premerge_NONMERGED.dta", replace

* Now merge silverchain first 
use "$data/rcbh_SC_premerge_NONMERGED.dta", clear 
tostring yob, replace
merge 1:1 post_code yob mob sex practice_id using  "$data/rcbh_SC_nonmerged.dta" // 60 silverchain not merged
keep if _merge ==3

order grad_* withd* refer*
br if grad_1b_SC !=. | withdrawal_date_SC != "" | referral_date_SC !=. // manually keep those that match clearly across grad/refer/withd dates 

drop if polar_id_1 == "16458455" // dates 1 year apart
drop if polar_id_1 == "14775380"
drop if polar_id_1 == "5374614287"

drop _merge

save "$data/rcbh_SC_matched_NONMERGED.dta", replace 


****************************
* 6) Remaining EACH patients
****************************

use "$data/rcbh_SC_premerge_NONMERGED.dta", clear // from "Remaining silverchain patients" section above; can use same dataset for EACH 

tostring yob, replace
drop if post_code == "Unknown"
destring post_code, replace
merge 1:1 post_code yob mob sex practice_id using  "$data/rcbh_EACH_nonmerged.dta" // 171 merged; go through manually. Fun times :)
keep if _merge ==3
drop _merge

** Gen a manual flag; set to one for people we keep in data editor
br
g manual_flag =. // set to data editor mode in "browse"

order manual_flag ///
      grad_1b_EACH grad_2b_EACH grad_1b patientsitekey referral_date_EACH refer_1 withd* // seperate EACH and POLAR dates by patientsitekey for easier comparison
sort practice_id grad_1b
 // NOTE: most are obvious cases where GP would have gone on POLAR and flagged all as grad same day; these are all kept as POLAR dates not reliable. Only ~6 dropped overall 
save "$data/rcbh_EACH_matched_NONMERGED.dta", replace 
keep if manual_flag ==1
save "$data/rcbh_EACH_matched_NONMERGED_flagmanual.dta", replace 

** Append residual EACH/SC onto main EACH/SC RCBH sample
use "$data/rcbh_EACH_SC_Appended.dta", clear

tostring post_code, replace
append using "$data/rcbh_SC_matched_NONMERGED.dta"
drop grad_1b grad_2b withd_1b withd_2b refer_1 N flag_each flag_silverchain enrolled_1b enrolled_2b enrolled_3b enrolled_4b

destring post_code, replace
append using "$data/rcbh_EACH_matched_NONMERGED_flagmanual.dta"

drop manual_flag grad_1b refer_1 withd_1b withd_2b N flag_each flag_silverchain grad_2b enrolled_1b ///
enrolled_2b enrolled_3b enrolled_4b 

save "$data/rcbh_EACH_SC_Appended_Twice.dta", replace // 984 patients total here 
                                                      // (276 original silverchain + 405 EACH + 30 additional silverchain, minus couple due to no diagnosis etc)


													  
************************************************
* 7) Cross-link with POLAR diagnosis information
************************************************													  
													  
use "$data/rcbh_EACH_SC_Appended_Twice.dta", clear

*Merge in diagnosis
merge 1:1 polar_id_1 using "$data/diagnosis_sept_cleaned_final.dta"
keep if _merge ==3 | _merge ==1 // 373 with >2022q1 diagnosis date patients no diagnosis in POLAR; may have in monthly dataset 

replace cardio_SC_max =1 if cardio_max==1 & cardio_SC_max !=.
replace cardio_each_max=1 if cardio_max==1 & cardio_each_max !=.

replace resp_SC_max =1 if respiratory_max ==1 & resp_SC_max !=.
replace resp_each_max =1 if respiratory_max ==1 & resp_each_max !=.

replace frailty_SC_max =1 if (muscul_max ==1) & frailty_SC_max !=.
replace frailty_each_max =1 if (muscul_max ==1) & frailty_each_max !=.

drop cardio_max respiratory_max muscul_max

* gen final single diagnosis vars with silverchain/each cross-linked
g cardio_max = (cardio_SC_max ==1 | cardio_each_max==1)
g resp_max = (resp_SC_max ==1 | resp_each_max==1)
g frailty_max = (frailty_SC_max ==1 | frailty_each_max==1)

* identify patients without 3 main conditions 
g no_cond = (cardio_max ==0 & resp_max==0 & frailty_max==0) // 50 people (note: if we merge in diagnosis <2022m1, they prob be filled)

drop diag_date* _merge diag_month *each_max *SC_max

**** DECIDE DROP NON-MAIN CONDITIONS FOR TREATMENT GROUP
drop cancer_max mental_max diab_max disability_max dementia_alzheim_max // NOTE: decided to do this because for treatment group they're not defined reliably 
                                                                        // because ~300 treatment dont exist in diag data >=2022m1; but can get their main 3 conditions from provider data
save "$data/rcbh_sample_precontrol.dta", replace											  
													  													  
													  
*******************************************
* 8) Create final sample with control group
*******************************************

** Get eligible control group 
use "$data/patientflag_sept.dta", clear

split(patientsitekey),p("-")
destring patientsitekey1, replace
drop if inlist(patientsitekey1, 1083, 1085, 688) // 1668 rows; remove this from control group dif practices
				
merge m:1 patientsitekey using "$data/patient_sept", keepusing(polar_id_1 yob mob post_code sex)
keep if _merge ==3 
drop _merge

drop if post_code == "Unknown" // 5635 rows
destring post_code, replace
merge m:1 polar_id_1 using "$data/rcbh_sample_precontrol.dta" // merged in treated; all matched, obviously
keep if _merge ==3 | flag_status == "RCBH Eligible" 
drop _merge 

sort polar_id_1 
order polar_id_1 

gen dt_tm  = clock(flag_datetime, "YMDhms")
gen    d_day  = dofc(dt_tm)
format d_day %td

* first treatment dates (monthly and quarterly)
g treat_date_EACH = mofd(enrolled_1b_EACH)
format treat_date_EACH %tm
g treat_date_SC = mofd(enrolled_1b_SC)
format treat_date_SC %tm
bys polar_id_1: egen first_treat = min(treat_date_EACH)
replace first_treat = treat_date_SC if first_treat ==.

bys polar_id_1: egen first_enrol = min(first_treat)
format first_enrol %tm
gen first_enrol_q = qofd(dofm(first_enrol))
format first_enrol_q  %tq

drop first_treat treat_date_SC treat_date_EACH

order polar_id_1 patientsitekey first_enrol first_enrol_q 

* Identify all patients that have weak graduation dates 
g prog_duration_EACH = grad_1b_EACH - enrolled_1b_EACH // min 28 days; 
g prog_duration_SC = grad_1b_SC - enrolled_1b_SC // min 20 days; just keep it  
drop prog_duration_EACH prog_duration_SC

* Identify all patients that withdrew 
g prog_withd_EACH = withd_1b_EACH - enrolled_1b_EACH
br if prog_withd_EACH <=20 
drop if prog_withd_EACH <=3
br if withdrawal_date_SC != "" 
duplicates drop polar_id_1, force 

destring yob, force replace 
g age2 = 2025-yob
keep if age2>=18 & age2 !=.   
drop age2               
  
drop flag_id flag_name flag_description flag_datetime flag_status 

merge m:1 post_code using "$data/SEIFA_irsd.dta"
br if _merge !=3 
keep if _merge ==3
drop _merge

drop dt_tm d_day prog_withd_EACH post_code_pop 

g treated = first_enrol !=.

* Get the diagnoses for control group 
foreach var of varlist cardio_max resp_max frailty_max {
	rename `var' `var'2, replace
}

merge 1:1 polar_id_1 using  "$data/diagnosis_sept_cleaned_final.dta"
keep if _merge ==3 | treated ==1 // note: 22,102 dropped as diagnoses defined <2022m1

rename muscul_max frailty_max
rename respiratory_max resp_max
foreach var of varlist cardio_max resp_max frailty_max  {
	replace `var'2 =`var' if treated ==0
}

drop cardio_max resp_max frailty_max 
rename cardio_max2 cardio_max 
rename resp_max2 resp_max 
rename frailty_max2 frailty_max 

drop _merge

compress

* Merge in year/month of death 
merge 1:m polar_id_1 using "$data/patient_sept", keepusing(yod mod) // merge year/month of death
keep if _merge ==3

ta yod // ~650 have died in our treatment window; some also before in a way that should be impossible lol

g yod_exists = (yod != "NULL")
g yod_clean = yod if yod_exists ==1
g mod_clean = mod if yod_exists ==1
destring yod_clean, replace 
destring mod_clean, replace
drop yod mod yod_exists
gen qdate = qofd( dofm( ym(yod_clean, mod_clean) ) ) // year-quarterly death date var
format qdate %tq
rename qdate death_date
drop yod_clean mod_clean 

bys polar_id_1: egen date_death = min(death_date)
format date_death %tq 
drop death_date 

duplicates drop polar_id_1, force // this is fine

drop _merge

drop if first_enrol_q == tq(2025q4) // october enrolments cant use

g age = 2025-yob

* DROP AGE OUTLIERS; keep only age <= 3 SD away from treated group's mean
preserve
duplicates drop polar_id_1, force
su age if treated ==1  // mean = 79.82176, SD = 8.991313; 8.991313*3 = 26.973939; di 79.82176-26.973939 = 52.847821 --> RULE: keep if age >=53
restore
keep if age >=53

compress

save "$data/RCBH_sample_final.dta", replace 


*****************************
* 9) Coarsened exact matching
*****************************
		
use "$data/RCBH_sample_final.dta", clear

* matching vars:
keep polar_id_1 age post_code sex cardio_max resp_max frailty_max treated 

encode sex, gen(sex2) // not needed
drop if sex2 ==3 | sex2 ==4 // 8 people in control group non male or female

* Run CEM
cem age (15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95 100 105 110) ///
    sex2 post_code cardio_max resp_max frailty_max, treatment(treated) 

keep if cem_matched ==1 // 19095 remaining (18,373 control, 722 treated); it dropped 2 treated patients

save "$data/cem_results.dta", replace

use "$data/RCBH_sample_final.dta", clear 
merge 1:1 polar_id_1 using "$data/cem_results.dta"
keep if _merge ==3 // dropped 15,952 controls not matched (note: 11,252 if its irsd_quintile instead of deciles)
drop _merge 

save "$data/RCBH_sample_final_CEM.dta", replace
		

    
*=========================================
 * POLAR Services data
*=========================================	  

		      ****************************
              * Services dataset preclean
              ****************************

use "$data/PHN202_UoM_RCBH_Service", clear
destring practice_id, replace
append using "$data/service_sept.dta" // append latest september service data
keep patientsitekey service_date service_item providersitekey

* Dates 
**do this first to save merging memory
gen dt = clock(service_date, "YMDhms")
gen  date_daily = dofc(dt)
gen my = mofd(date_daily)
format my %tm
gen dy = dofd(date_daily)
format dy %td

drop if my < ym(2022,1) | my > ym(2025,9) // drops tiny bit of october data; we dont use its fine

compress

* Merge in RCBH patients and eligible 
merge m:1 patientsitekey using "$data/patient_sept", keepusing(polar_id_1)
keep if _merge ==3
drop _merge

merge m:1 polar_id_1 using "$data/RCBH_sample_final.dta", keepusing(treated)
ta treated if _merge ==2 // 1 rcbh patient no services (polar_id: 23438095)
bys polar_id_1: egen rcbh_flag = min(treated)
order polar_id_1 patientsitekey rcbh_flag

keep if _merge ==3  // drops 1 rcbh treated (polar id above)
drop _merge
			 
browse if regexm(service_item,"[A-Za-z]") //just have to delete
drop if regexm(service_item,"[A-Za-z]")	& rcbh_flag !=1		

browse if regexm(service_item,"[A-Za-z]") & rcbh_flag ==1		
drop if regexm(service_item,"[A-Za-z]") & rcbh_flag ==1	 // delete; all them have other services anyway		 

* Destring service item
destring service_item, gen(service_item_num) force
br if service_item_num ==. // there are double item combinations e.g. "5028/1" 

* Drop invalid service items
br if service_item_num ==0 & rcbh_flag ==1 // 424 rows from 118 unique patients; all still have services
drop if service_item_num ==0 // 25,692 rows; no service "0" exists? 

* Split rows with double item combinations into two rows
gen long _rowid = _n // unique row id to keep track during expansion
gen strL _items = subinstr(trim(service_item), "/", " ", .) // Turn "5043/1/99" into space-separated words and count them
gen byte _nitems = cond(missing(service_item), 0, wordcount(_items))
expand _nitems 
bysort _rowid: gen _k = _n /
replace service_item = word(_items, _k) 
drop _items _nitems _k _rowid

drop service_item_num // regen service_item_num with the additional double-item combinations
destring service_item, g(service_item_num) force
drop if service_item_num ==. 

keep if inrange(service_item_num, 1, 99901)

save "$data/RCBH_services_precollapse.dta", replace		
				
		      ****************************
              * Services dataset collapse
              ****************************	
use "$data/RCBH_services_precollapse.dta", clear

gen q = qofd(dofm(my)) 
format q %tq 

* Unique number of practices
split(patientsitekey),p("-")
bys polar_id_1 patientsitekey1 q : g prac_num = _n==1

* Unique number of providers 
bys polar_id_1 providersitekey q : g prov_num = _n==1

order polar_id_1 my patientsitekey rcbh_flag 
			  		 
* Number of telehealth services (any kind; some medical practioner in here)
gen byte service_tele_pooled = inlist(service_item_num, ///
    91790, 91800, 91801, 91802, 91920,  91890, 91891, 91900, 91910, 91920)  // general attendance + mymedicare + 92210(urgent; note: removed)

* Number of GP services
 gen byte service_gp_pooled = inlist(service_item_num, ///
    3, 23, 36, 44, 123,          /// general attendance in consulting rooms (Levels A–E)
    4, 24, 37, 47, 124,          /// general attendance out of consulting rooms (Levels A–E)
	5000, 5020, 5040, 5060, 5071,        /// after hours in consulting rooms
    5003, 5023, 5043, 5063, 5076,        /// after hours out of consulting rooms
    5010, 5028, 5049, 5067, 5077,        /// after hours RACF
    90020, 90035, 90043, 90051, 90054) // RACF (standard hours; Levels A–E)
	/*585, 588, 591, 594, 599, 600,   */     /// after hours urgent (inc. unsociable hours)
	

*CDM
// two new items replaced old >=july 2025
// 965 = prepare CCGPMP, 967 = review CCGPMP					                         
gen byte service_cdm_gpmp   = inlist(service_item_num, 721, 92024, 965, 92029)
gen byte service_cdm_tca    = inlist(service_item_num, 723, 92025)
gen byte service_cdm_review = inlist(service_item_num, 732, 92028, 967, 92030)
g byte service_cdm_mdmp      = inlist(service_item_num, 729,92026,731,92027)

* Practice nurse item 
g service_prac_nurse_complete = inlist(service_item_num, 10997, 10983, 10987) //
g service_prac_nurse_chronic = inlist(service_item_num, 10997, 93201, 93203) // chronic condition only
*Allied health assessments 
g byte service_alliedhealth = inrange(service_item_num, 10950, 10970)

* Proactive health assessments 
g service_gp_ha = inlist(service_item_num, 701, 703, 705, 707, 715)
g service_heart_health = inlist(service_item_num, 699)
						  
* Mental health plan
gen byte service_mhp = inlist(service_item_num, 2700,2701,2712,2713,2715,2717, ///
                              92112, 92113, 92116, 92117, 92114, 921126, 92115, 92127 )

* Multidisciplinary case conferencing	
g service_caseconf = inlist(service_item_num, 735, 739, 743, 747, 750, 758)		   

* Medication review 
g service_medreview = service_item_num ==900
			   
drop service_date service_item service_item_num 			   
collapse (sum) service_* prac_num prov_num, by(polar_id_1 q)			
			
save "$data/RCBH_services_quarterly.dta", replace	
			
			
*=========================================
 * POLAR REFERRALS
*=========================================
use "$data/phn202_uom_rcbh_referral.dta", clear
append using "$data/referral_sept.dta"

gen referral_dt_td = daily(substr(referral_date,1,10),"YMD")
format referral_dt_td %td
gen my = mofd(referral_dt_td)
format my %tm
drop referral_dt_td

drop if my < ym(2022,1) | my > ym(2025,9)

compress

merge m:1 patientsitekey using "$data/patient_sept.dta", keepusing(polar_id_1)
keep if _merge ==3
drop _merge

merge m:1 polar_id_1 using "$data/RCBH_sample_final.dta", keepusing(treated) 
keep if _merge==3 // 17 RCBH patients did not get a referral
drop _merge


g referral_number =1
g referral_number_HA_ED = (referral_category == "Hospital / Emergency Department")

g referral_cardio = inlist(referral_category, "Cardiology", "Vascular Surgery", "Neurology")
g referral_respiratory = inlist(referral_category, "Respiratory & Sleep Medicine", "Respiratory physician", "Thoracic")
g referral_frailty = inlist(referral_category, "Geriatrics")


drop referral_date referral_category
collapse (sum) referral_*, by(polar_id_1 my)

order polar_id_1 my
sort polar_id_1 my
save "$data/RCBH_referrals_monthly.dta", replace

gen q = qofd(dofm(my)) 
format q %tq 

collapse (sum) referral_*, by(polar_id_1 q)
save "$data/RCBH_referrals_quarterly.dta", replace


*=========================================
 * POLAR Prescriptions 
*=========================================
use patientsitekey prescription_date atc_code using "$data/PHN202_UoM_RCBH_Prescription_OY", clear
append using "$data/presc_sept.dta"
keep patientsitekey prescription_date atc_code

gen dt_td = daily(substr(prescription_date,1,10),"YMD")
format dt_td %td
gen my = mofd(dt_td)
format my %tm
drop dt_td

drop if my < ym(2022,1) | my > ym(2025,9)

compress
merge m:1 patientsitekey using "$data/patient_sept.dta", keepusing(polar_id_1)
keep if _merge==3
drop _merge

merge m:1 polar_id_1 using "$data/RCBH_sample_final.dta", keepusing(treated) 
keep if _merge==3 // note: xx RCBH patients no prescriptions
drop _merge

* ATC groups
drop if missing(atc_code) // 3 rows

gen atc_letter = substr(atc_code, 1, 1)

foreach L in A B C D G H J L M N P R S T V {
    gen byte presc_atc_`L' = (atc_letter=="`L'")
}

gen q = qofd(dofm(my)) 
format q %tq 

* Define Polypharmacy in seperate quarterly dataset
preserve 
egen pres_any = rowtotal(presc_atc_A presc_atc_B presc_atc_C presc_atc_D presc_atc_G presc_atc_H presc_atc_J presc_atc_L presc_atc_M presc_atc_N presc_atc_P presc_atc_R presc_atc_S presc_atc_V) // to avoid invalid string etc; condition on this
*Collapse by quarter instead 
/*bys polar_id_1 q atc_code: g n_presc_quart = _n==1 if pres_any !=0 */ // number of unique atc prescriptions 
collapse (sum) pres_any, by(polar_id_1 q)
save "$data/RCBH_polyphamracy.dta", replace
restore

keep polar_id_1 my presc_*
order polar_id_1 my 
sort polar_id_1 my

collapse (sum) presc_atc_*, by(polar_id_1 my)
save "$data/RCBH_prescriptions.dta", replace

gen q = qofd(dofm(my)) // do again here after first collapse
format q %tq 
	
collapse (sum) presc_atc_*, by(polar_id_1 q)
save "$data/RCBH_prescriptions_quarterly.dta", replace

*=========================================
 *Panel dataset construction
*=========================================	
	  	  
*gen empty quarterly dataset
clear
local start = yq(2022,1)
local stop  = yq(2025,3)
local N = `stop' - `start' + 1
set obs `N'
gen int q = `start' + _n - 1
format q %tq
list in 1/3
save "$data/rcbh_skeleton.dta", replace

*Gen complete panel
use "$data/RCBH_sample_final.dta", clear
cross using "$data/rcbh_skeleton.dta" // adds 'q' to current data

merge 1:1 polar_id_1 q using "$data/RCBH_services_quarterly.dta", nogen
merge 1:1 polar_id_1 q using "$data/RCBH_prescriptions_quarterly.dta", nogen
merge 1:1 polar_id_1 q using "$data/RCBH_referrals_quarterly.dta", nogen

* fill missing monthly counts with 0
ds service_*  referral_* presc_*
foreach v of varlist `r(varlist)' {
    replace `v' = 0 if missing(`v')
}	

drop patientsitekey2 diag_month diag_date* 
			
compress

order polar_id_1 q first_enrol first_enrol_q 
sort first_enrol polar_id_1 q

save "$data/rcbh_panel_quarterly.dta", replace


*=========================================
 * Analysis
*=========================================	


            **************************
            * Final pre-analysis clean
            **************************

use "$data/rcbh_panel_quarterly.dta", clear
sort polar_id_1 q

* merge prescription numbers
merge m:1 polar_id_1 q using "$data/RCBH_polyphamracy.dta", keepusing(pres_any) 
drop _merge 
ds pres_any
foreach v of varlist `r(varlist)' {
    replace `v' = 0 if missing(`v')
}

* Censor dead people 
g withd_dead = .
* EACH: datetime -> daily (if withd_1b_EACH is already %td, drop dofd())
replace withd_dead = dofd(withd_1b_EACH) if withdrawal_reason=="Died" & !missing(withd_1b_EACH)
* SC: convert string to daily
gen double sc_dt = date(withdrawal_date_SC, "DMY")
format sc_dt %td
replace withd_dead = sc_dt if missing(withd_dead) & sc_dt !=.
format withd_dead %td
*Quarter of death and censor
gen q_dead = qofd(withd_dead)
replace q_dead = date_death if date_death !=. & treated ==0 // control group deaths
format q_dead %tq
drop if q_dead < tq(2022q1) // 75 unique controls pre study period.... invalid anyhow
drop if q >= q_dead & q_dead !=.

drop patientsitekey patientsitekey1 mob cancer_max mental_max diab_max disability_max dementia_alzheim_max
destring seifa_irsd, replace 

** Gen paper outcome variables (note: referrals and prescriptions already generated)
*1) gp consultations (f2f + telehealth)
g gp_face_tele = service_tele_pooled + service_gp_pooled 

*2) Multidisciplinary/care coordination
g md_services = service_cdm_tca + service_cdm_mdmp + service_caseconf + service_medreview + service_gp_ha ///
 + service_prac_nurse_chronic + service_cdm_gpmp + service_cdm_review + service_mhp // full item
g md_services2 = service_cdm_tca + service_cdm_mdmp + service_caseconf + service_medreview  + service_gp_ha ///
+ service_prac_nurse_chronic  + service_cdm_review  // no mhp & gpmp; trends work

*** Seifa Heterogeneity Analysis
xtile seifa_med = seifa_irsd, n(2)
label define seifa 1 "Low" 2 "High"
label values seifa_med seifa

* Sex heterogeneity
encode sex, gen(sex2) 
drop if sex2 ==3 | sex2 ==4 // 8 people in control group non male or female
label define sex 1 "Male" 2 "Female"
label values sex2 sex

* Age heterogeneity
preserve
duplicates drop polar_id_1,force 
su age if treated==1, de // median is 81 (for both matched and unmatched sample)
restore
g age_med = age >=81
label define age 0 "<81" 1 ">=81"
label values age_med age 

* Prog duration heterogeneity 
g prog_duration_EACH = grad_1b_EACH  - enrolled_1b_EACH 
g prog_duration_SC =  grad_1b_SC - enrolled_1b_SC 

g prog_duration_all = .
replace prog_duration_all = prog_duration_EACH
replace prog_duration_all = prog_duration_SC if prog_duration_EACH ==.

* deal ungrad people 
g days_enrolled_EACH = td(1oct2025) - enrolled_1b_EACH
g days_enrolled_SC= td(1oct2025) - enrolled_1b_SC

replace prog_duration_all = days_enrolled_SC if days_enrolled_SC !=. & prog_duration_SC ==.
replace prog_duration_all = days_enrolled_EACH if days_enrolled_EACH !=. & prog_duration_EACH ==.

xtile duration = prog_duration_all, n(2)
replace duration = 3 if prog_duration_all ==.
label define duration 1 "below median" 2 "above median" 3 "Never treated"
label values duration duration

** gen DiD variables (ID, time var, treatment)
* ID variable
egen ID   = group(polar_id_1)
* Time variable
egen tvar = group(q)   // same tvar for everyone in the same calendar quarter
* Treatment; experience variations t = -3, -4, -5
g trel= q - first_enrol_q
g treat = 0
replace treat = 1 if trel >= -4 & first_enrol_q !=. // t = -5 reference

* ln(0.10 + outcome) robustness check
 foreach v in gp_face_tele referral_number md_services md_services2 pres_any {
g `v'_LN =ln(`v'+0.1)
 }
 	

order ID tvar treat first_enrol_q q q_dead sex post_code age  


            **************************
            *   Baseline Model DiD
            **************************
			
	local outcomes gp_face_tele referral_number md_services  pres_any
    foreach y of local outcomes {
        local gname did_`y'
        did_multiplegt_dyn `y' ID tvar treat, ///
            effects(10) placebo(5) cluster(ID) ///
            save_results($output2/`gname'_baseline`tag')
    }			
			
			
            **************************
            *  All models DiD
            **************************
			
-------------------------------------------------------------
* 0. NOTES on DiD code
*------------------------------------------------------------
*1) for the unmatched spec, we run both "raw" and "LN" models
*2) for the matched spec, we run only "raw" (don't need Log)
*3) Swap between directory $output and $output2 for matched and umatched results, respectively
*4) For the silverchain vs EACH for report, oscilate between the drop "flag" code at the start of the estimation command

*------------------------------------------------------------
* 1. Define base (non-log) outcomes
*------------------------------------------------------------
local base_outcomes gp_face_tele referral_number md_services md_services2 pres_any

*------------------------------------------------------------
* 2. Loop over raw only outcomes
*------------------------------------------------------------
foreach version in raw LN {

    if "`version'" == "raw" {
        local vsuf ""
        local tag  ""          // filenames with no suffix
    }
    else if ("`version'" == "LN") {
        local vsuf "_LN"
        local tag  "_LN"       // filenames end in _LN
    }

    * Build list of outcomes for this version
    local outcomes ""
    foreach b of local base_outcomes {
        local outcomes `outcomes' `b'`vsuf'
    }

    *--------------------------------------------------------
    * Baseline results
    *--------------------------------------------------------
    foreach y of local outcomes {
        local gname did_`y'
        did_multiplegt_dyn `y' ID tvar treat, ///
            effects(10) placebo(5) cluster(ID) ///
            save_results($output2/`gname'_baseline`tag')
    }

    *--------------------------------------------------------
    * Sex heterogeneity
    *--------------------------------------------------------
    foreach y of local outcomes {
        local gname did_`y'
        did_multiplegt_dyn `y' ID tvar treat, ///
            effects(10) placebo(5) cluster(ID) by(sex2) ///
            save_results($output2/`gname'_sex`tag')
    }

    *--------------------------------------------------------
    * SEIFA heterogeneity
    *--------------------------------------------------------
    foreach y of local outcomes {
        local gname did_`y'
        did_multiplegt_dyn `y' ID tvar treat, ///
            effects(10) placebo(5) cluster(ID) by(seifa_med) ///
            save_results($output2/`gname'_seifa`tag')
    }

    *--------------------------------------------------------
    * Age heterogeneity
    *--------------------------------------------------------
    foreach y of local outcomes {
        local gname did_`y'
        did_multiplegt_dyn `y' ID tvar treat, ///
            effects(10) placebo(5) cluster(ID) by(age_med) ///
            save_results($output2/`gname'_age`tag')
    }

    *--------------------------------------------------------
    * Program duration heterogeneity (duration 2 or 3)
	 **note: re-run for this only by duration 1 or 3
    *--------------------------------------------------------
    foreach y of local outcomes {
        local gname did_`y'
        did_multiplegt_dyn `y' ID tvar treat ///
            if duration == 2 | duration == 3, ///
            effects(10) placebo(5) cluster(ID) ///
            save_results($output2/`gname'_duration23`tag')
    }

}


*------------------------------------------------------------
* Do baseline model
*------------------------------------------------------------
local base_outcomes gp_face_tele referral_number md_services pres_any

foreach version in raw {

    if "`version'" == "raw" {
        local vsuf ""
        local tag  ""          // filenames with no suffix
    }
    else if ("`version'" == "LN") {
        local vsuf "_LN"
        local tag  "_LN"       // filenames end in _LN
    }

    * Build list of outcomes for this version
    local outcomes ""
    foreach b of local base_outcomes {
        local outcomes `outcomes' `b'`vsuf'
    }

    *--------------------------------------------------------
    * Baseline results
    *--------------------------------------------------------
    foreach y of local outcomes {
        local gname did_`y'
        did_multiplegt_dyn `y' ID tvar treat, ///
            effects(10) placebo(5) cluster(ID) ///
            save_results($output2/`gname'_baseline`tag')
    }

}


*------------------------------------------------------------
* Log model for baseline outcomes in unmatched model
*------------------------------------------------------------

foreach version in LN {

    if "`version'" == "raw" {
        local vsuf ""
        local tag  ""          // filenames with no suffix
    }
    else if ("`version'" == "LN") {
        local vsuf "_LN"
        local tag  "_LN"       // filenames end in _LN
    }

    * Build list of outcomes for this version
    local outcomes ""
    foreach b of local base_outcomes {
        local outcomes `outcomes' `b'`vsuf'
    }

    *--------------------------------------------------------
    * Baseline results
    *--------------------------------------------------------
    foreach y of local outcomes {
        local gname did_`y'
        did_multiplegt_dyn `y' ID tvar treat, ///
            effects(10) placebo(5) cluster(ID) ///
            save_results($output/`gname'_baseline`tag')
    }
}

*------------------------------------------------------------
*  Duration seperately
*------------------------------------------------------------
local base_outcomes referral_number gp_face_tele md_services pres_any

drop if grad_1b_SC ==. & enrolled_1b_SC !=.
drop if grad_1b_EACH ==. & enrolled_1b_EACH !=.

foreach version in raw {

    if "`version'" == "raw" {
        local vsuf ""
        local tag  ""          // filenames with no suffix
    }
    else if ("`version'" == "LN") {
        local vsuf "_LN"
        local tag  "_LN"       // filenames end in _LN
    }

    * Build list of outcomes for this version
    local outcomes ""
    foreach b of local base_outcomes {
        local outcomes `outcomes' `b'`vsuf'
    }

    *--------------------------------------------------------
    * Program duration heterogeneity (duration 1 or 3)
	** change to 2 or 3 below also for that specification
    *--------------------------------------------------------
    foreach y of local outcomes {
        local gname did_`y'
        did_multiplegt_dyn `y' ID tvar treat ///
            if duration == 2 | duration == 3, ///
            effects(10) placebo(5) cluster(ID) ///
            save_results($output2/`gname'_duration23`tag')
    }

}

duplicates drop polar_id_1, force

su prog_duration_all if treated ==1, de
su prog_duration_all if treated ==1 & enrolled_1b_SC !=., de
su prog_duration_all if treated ==1 & enrolled_1b_EACH !=., de			
			

			
