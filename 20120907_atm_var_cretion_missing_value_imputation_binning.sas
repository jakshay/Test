options obs =max errors = 2 mergenoby = error symbolgen macrogen; 

libname tom  '/cbna/cbnarisk/ts90692/data';
libname atm_in '/cbna/cbnarisk/Essex/atm/data';
%include '/cbna/cbnarisk/Essex/Teradata_logon/useridpwd.sas';
libname frd TERADATA user=&uid. password=&pwd.  tdpid='edwprod' database=P_CBNA_FR_ALL_V_C;
libname atmloc '/cbna/cbnarisk/Essex/atm/analysis/atm_location';
libname atm_perf '/cbna/cbnarisk/Essex/atm/data/atm_three_months_perf';
libname atm_bins '/cbna/cbnarisk/Essex/atm/data/cart_bins_datasets';
libname crtdta '/cbna/cbnarisk/Essex/atm/data/atm_cart_datasets';

*******************************************************************************************************************************************;
*******************************************************************************************************************************************;
*******************************************************************************************************************************************;
*******************************************************************************************************************************************;
********* 0. Pulling First party fraud tags                                                                                       *********;
********* 1. Missing value treatment of variables                                                                                 *********;
********* 2. Create performance based in the fly variables in the dataset                                                         *********;
********* 3. Missing value treatment of variables continued for created variables                                                 *********;
********* 4. BINning treatment of variables                                                                                       *********;
*******************************************************************************************************************************************;
*******************************************************************************************************************************************;

*******************************************************************************************************************************************;
********* 0. Pulling First party fraud tags                                                                                       *********;
*******************************************************************************************************************************************;

 proc sql;
       create table atm_june_alerts as
       select 
       EFD_TRANS_ID            ,
       ALERT_IDX			   
      from frd.trans_efd_atomic
       where trans_date GE '01jun12'd and trans_date LE '30jun12'd 
       and EFD_TRANS_TYPE in ('Cash Withdrawal');
   quit;

data temp (keep  = ALERT_IDX);
set atm_june_alerts (keep = EFD_TRANS_ID ALERT_IDX);
if ALERT_IDX NE "";
run;

proc sort data=temp nodupkey;
by ALERT_IDX;
run;

proc sql;
       create table efd_alerts as
       select *  
       from frd.EFD_ALERT a, temp b
       where a.ALERT_IDX = b.ALERT_IDX;
quit;

proc sort data=efd_alerts;
by ALERT_IDX last_mod_datetime;
run;

data efd_alerts (keep = ALERT_IDX ALERT_STATUS_IDX);
set efd_alerts;
by ALERT_IDX;
if last.ALERT_IDX;
run;

data atm_june_alerts;
set atm_june_alerts (keep = EFD_TRANS_ID ALERT_IDX);
if ALERT_IDX NE "";
run;

proc sort data=atm_june_alerts;
by ALERT_IDX;
run;

data  atm_june_alerts (keep = EFD_TRANS_ID ALERT_IDX ALERT_STATUS_IDX mer_ind);
merge atm_june_alerts (in=a) efd_alerts (in=b);
by ALERT_IDX;
if a;
mer_ind=compress(a||b);
run;

proc freq data=atm_june_alerts;
tables mer_ind /list missing;
run;

proc sort data=atm_june_alerts;
by EFD_TRANS_ID;
run;

data june_temp_Dataset;
merge atm_perf.june_temp_Dataset (in=a) atm_june_alerts (in=b);
by EFD_TRANS_ID;
if a;
if compress(ALERT_STATUS_IDX) = compress("Confirmed Fraud Abuse") then First_party_Tag = 1; else first_party_Tag=0;
** first party fraud to be excludedd ** ;
Final_fraud_Tag=fraud_tag_com * (1-first_party_Tag);
run;

proc freq data=june_temp_Dataset;
tables First_party_Tag First_party_Tag*fraud_tag_com Final_fraud_Tag Final_fraud_Tag*fraud_tag_com /list missing;
run;

*** june_temp_Dataset being generated from 	20120615_atm_overlap_phase1_limit_reduction.sas file in limit analysis folder *** ;
data crtdta.june_score_less_40;
set june_temp_Dataset;

*******************************************************************************************************************************************;
********* 1. Missing value treatment of variables                                                                                 *********;
*******************************************************************************************************************************************;
if ACCT_BAL_AMT                    = (.) then ACCT_BAL_AMT                   =0;
if Cnt_trans                       = (.) then Cnt_trans                      =0;
if Max_DOL_AMT                     = (.) then Max_DOL_AMT                    =0;
if Mean_DOL_AMT                    = (.) then Mean_DOL_AMT                   =0;
if Max_ACTIMIZE_CARD_SCORE         = (.) then Max_ACTIMIZE_CARD_SCORE        =0;
if Mean_ACTIMIZE_CARD_SCORE        = (.) then Mean_ACTIMIZE_CARD_SCORE       =0;
if trans_30_0                      = (.) then trans_30_0                     =0;
if trans_30_1_4                    = (.) then trans_30_1_4                   =0;
if trans_30_5_6                    = (.) then trans_30_5_6                   =0;
if trans_30_0_6                    = (.) then trans_30_0_6                   =0;
if trans_30_rest                   = (.) then trans_30_rest                  =0;
if dol_30_200                      = (.) then dol_30_200                     =0;
if dol_30_200_500                  = (.) then dol_30_200_500                 =0;
if dol_30_500_800                  = (.) then dol_30_500_800                 =0;
if dol_30_800_plus                 = (.) then dol_30_800_plus                =0;
if dol_30_500_plus                 = (.) then dol_30_500_plus                =0;
if dol_30_200_800                  = (.) then dol_30_200_800                 =0;
if on_us_30                        = (.) then on_us_30                       =0;
if off_us_30                       = (.) then off_us_30                      =0;
if intl_flag_30                    = (.) then intl_flag_30                   =0;
if score_30_untriggered            = (.) then score_30_untriggered           =0;
if score_30_40_plus                = (.) then score_30_40_plus               =0;
if score_30_med                    = (.) then score_30_med                   =0;
if score_30_high                   = (.) then score_30_high                  =0;
if score_30_vhigh                  = (.) then score_30_vhigh                 =0;
if var_24                          = (.) then var_24                         =0;
if Mean_ACTIMIZE_CARD_SCORE_24     = (.) then Mean_ACTIMIZE_CARD_SCORE_24    =0;
if Max_ACTIMIZE_CARD_SCORE_24      = (.) then Max_ACTIMIZE_CARD_SCORE_24     =0;
if mean_DOL_AMT_24                 = (.) then mean_DOL_AMT_24                =0;
if max_DOL_AMT_24                  = (.) then max_DOL_AMT_24                 =0;
if sum_DOL_AMT_24                  = (.) then sum_DOL_AMT_24                 =0;
if trans_24_0                      = (.) then trans_24_0                     =0;
if trans_24_1_4                    = (.) then trans_24_1_4                   =0;
if trans_24_5_6                    = (.) then trans_24_5_6                   =0;
if trans_24_0_6                    = (.) then trans_24_0_6                   =0;
if trans_24_rest                   = (.) then trans_24_rest                  =0;
if dol_24_200                      = (.) then dol_24_200                     =0;
if dol_24_200_500                  = (.) then dol_24_200_500                 =0;
if dol_24_500_800                  = (.) then dol_24_500_800                 =0;
if dol_24_800_plus                 = (.) then dol_24_800_plus                =0;
if dol_24_500_plus                 = (.) then dol_24_500_plus                =0;
if dol_24_200_800                  = (.) then dol_24_200_800                 =0;
if on_us_24                        = (.) then on_us_24                       =0;
if off_us_24                       = (.) then off_us_24                      =0;
if intl_flag_24                    = (.) then intl_flag_24                   =0;
if score_24_untriggered            = (.) then score_24_untriggered           =0;
if score_24_40_plus                = (.) then score_24_40_plus               =0;
if score_24_med                    = (.) then score_24_med                   =0;
if score_24_high                   = (.) then score_24_high                  =0;
if score_24_vhigh                  = (.) then score_24_vhigh                 =0;
if var_5                           = (.) then var_5                          =0;
if mean_ACTIMIZE_CARD_SCORE_5      = (.) then mean_ACTIMIZE_CARD_SCORE_5     =0;
if max_ACTIMIZE_CARD_SCORE_5       = (.) then max_ACTIMIZE_CARD_SCORE_5      =0;
if mean_DOL_AMT_5                  = (.) then mean_DOL_AMT_5                 =0;
if max_DOL_AMT_5                   = (.) then max_DOL_AMT_5                  =0;
if sum_DOL_AMT_5                   = (.) then sum_DOL_AMT_5                  =0;
if trans_5_0                       = (.) then trans_5_0                      =0;
if trans_5_1_4                     = (.) then trans_5_1_4                    =0;
if trans_5_5_6                     = (.) then trans_5_5_6                    =0;
if trans_5_0_6                     = (.) then trans_5_0_6                    =0;
if trans_5_rest                    = (.) then trans_5_rest                   =0;
if dol_5_200                       = (.) then dol_5_200                      =0;
if dol_5_200_500                   = (.) then dol_5_200_500                  =0;
if dol_5_500_800                   = (.) then dol_5_500_800                  =0;
if dol_5_800_plus                  = (.) then dol_5_800_plus                 =0;
if dol_5_500_plus                  = (.) then dol_5_500_plus                 =0;
if dol_5_200_800                   = (.) then dol_5_200_800                  =0;
if on_us_5                         = (.) then on_us_5                        =0;
if off_us_5                        = (.) then off_us_5                       =0;
if intl_flag_5                     = (.) then intl_flag_5                    =0;
if score_5_untriggered             = (.) then score_5_untriggered            =0;
if score_5_40_plus                 = (.) then score_5_40_plus                =0;
if score_5_med                     = (.) then score_5_med                    =0;
if score_5_high                    = (.) then score_5_high                   =0;
if score_5_vhigh                   = (.) then score_5_vhigh                  =0;
if last_ACCT_BAL_AMT               = (.) then last_ACCT_BAL_AMT              =0;
if Last_DOL_AMT                    = (.) then Last_DOL_AMT                   =0;
if rjt_var_1                       = (.) then rjt_var_1                      =0;
if rjt_dol_1_200                   = (.) then rjt_dol_1_200                  =0;
if rjt_dol_1_200_500               = (.) then rjt_dol_1_200_500              =0;
if rjt_dol_1_500_800               = (.) then rjt_dol_1_500_800              =0;
if rjt_dol_1_800_plus              = (.) then rjt_dol_1_800_plus             =0;
if rjt_on_us_1                     = (.) then rjt_on_us_1                    =0;
if rjt_off_us_1                    = (.) then rjt_off_us_1                   =0;
if rjt_intl_flag_1                 = (.) then rjt_intl_flag_1                =0;
if rjt_var_24                      = (.) then rjt_var_24                     =0;
if rjt_dol_24_200                  = (.) then rjt_dol_24_200                 =0;
if rjt_dol_24_200_500              = (.) then rjt_dol_24_200_500             =0;
if rjt_dol_24_500_800              = (.) then rjt_dol_24_500_800             =0;
if rjt_dol_24_800_plus             = (.) then rjt_dol_24_800_plus            =0;
if rjt_on_us_24                    = (.) then rjt_on_us_24                   =0;
if rjt_off_us_24                   = (.) then rjt_off_us_24                  =0;
if rjt_intl_flag_24                = (.) then rjt_intl_flag_24               =0;
if nonciti_fraud_count_atm         = (.) then nonciti_fraud_count_atm        =0;
if same_day_trans                  = (.) then same_day_trans                 =0;
if Sum_Same_day_withdrawal         = (.) then Sum_Same_day_withdrawal        =0;
if max_daily_withdrawal_30         = (.) then max_daily_withdrawal_30        =0;
if limit_withdrwl_max_90days       = (.) then limit_withdrwl_max_90days      =0;
if trans_today_pct_30days          = (.) then trans_today_pct_30days         =0;
if limit_same_day_exlc_curr        = (.) then limit_same_day_exlc_curr       =0;
if with_pct                        = (.) then with_pct                       =0;
if with_pct_one_day                = (.) then with_pct_one_day               =0;

*******************************************************************************************************************************************;
********* 1. Create performance based in the fly variables in the dataset                                                         *********;
*******************************************************************************************************************************************;
mob = datepart(TRANS_LCL_DATE_TIME) - EFD_ACCT_OPEN_DT;

dev_last_score = (ACTIMIZE_CARD_SCORE > Last_ACTIMIZE_CARD_SCORE)*1;

** Last_DOL_AMT : var skipped as might not lead to much benefit ;
dev_last_TRANS_LCL_DATE_TIME = TRANS_LCL_DATE_TIME - last_TRANS_LCL_DATE_TIME;

if last_ACCT_BAL_AMT NE 0 then dev_last_ACCT_BAL_AMT=(ACCT_BAL_AMT)/last_ACCT_BAL_AMT; else dev_last_ACCT_BAL_AMT=0;
dev_last_EFD_CHAN_DESC =  (EFD_CHAN_DESC = last_EFD_CHAN_DESC )*1;
dev_last_ATM_CNTRY_CODE = (ATM_CNTRY_CODE = last_ATM_CNTRY_CODE)*1;
dev_last_ATM_CITY       = ( ATM_CITY     = last_ATM_CITY      )*1;     
dev_last_ATM_ZIP_CODE   = ( ATM_ZIP_CODE = last_ATM_ZIP_CODE  )*1;
dev_last_ATM_ST_CODE    = ( ATM_ST_CODE  = last_ATM_ST_CODE   )*1;

if Cnt_trans NE 0 then trans_today_pct_30days=(var_24/Cnt_trans)*100; else trans_today_pct_30days=0;

bin_max_dollar_indic= (dol_amt > Max_DOL_AMT)*1; 

dev_mean_score=	ACTIMIZE_CARD_SCORE - Mean_ACTIMIZE_CARD_SCORE;

bin_max_score_indic= (ACTIMIZE_CARD_SCORE > Max_ACTIMIZE_CARD_SCORE)*1; 

limit_24hrs_incl_curr = sum(of sum_DOL_AMT_24 DOL_AMT);

limit_same_day_exlc_curr = Sum_Same_day_withdrawal;

limit_same_day_incl_curr = sum(of Sum_Same_day_withdrawal DOL_AMT);

limit_max_30days_indic_exc_curr =  (Sum_Same_day_withdrawal >= max_daily_withdrawal_30)*1;
limit_max_30days_indic_inc_curr =  (Sum_Same_day_withdrawal+DOL_AMT >= max_daily_withdrawal_30)*1;

limit_max_90days_indic_exc_curr = (Sum_Same_day_withdrawal >= limit_withdrwl_max_90days)*1;
limit_max_90days_indic_inc_curr = (Sum_Same_day_withdrawal+DOL_AMT  >= limit_withdrwl_max_90days)*1;

if ACCT_BAL_AMT NE 0 then with_pct = (dol_amt/ACCT_BAL_AMT)*100; else with_pct = 0 ;

if ACCT_BAL_AMT NE 0 then with_pct_one_day =	 sum(of Sum_Same_day_withdrawal DOL_AMT)*100/(ACCT_BAL_AMT); else with_pct_one_day=0;

length opp_class $25;

Local_HOUR =hour(TRANS_LCL_DATE_TIME); 
if EFD_CHAN_DESC = "Citi ATM" then citi_tag = 1;
else citi_tag = 0;

** citi - atm rules *** ;

*******Upfront Declines**********;
if 80 <= ACTIMIZE_CARD_SCORE <= 100
and 1 <=Local_HOUR <=6
and citi_tag = 1
then opp_class = "Citi Decline";

else if 50 <= ACTIMIZE_CARD_SCORE <=70 
    and 1 <=   Local_HOUR <=  4
    and dol_amt > = 200
	and citi_tag = 1
then opp_class = "Citi Decline";

else if ACTIMIZE_CARD_SCORE = 40
    and 1 <= Local_HOUR <= 4
    and dol_amt >= 800
	and citi_tag = 1
    then opp_class = "Citi Decline";

*******Immediate Action**********;

else if 80 <= ACTIMIZE_CARD_SCORE <= 100
    and Local_HOUR  = 0 
	and citi_tag = 1
then opp_class = "Citi Action";

else if  50 <= ACTIMIZE_CARD_SCORE <= 70 
    and Local_HOUR =  0
    and dol_amt > = 800
	and citi_tag = 1
then opp_class = "Citi Action";

else if 50 <= ACTIMIZE_CARD_SCORE <= 70 
    and 1 <= Local_HOUR <= 4
    and dol_amt < 200
	and citi_tag = 1
then opp_class = "Citi Action";

else if 50 <= ACTIMIZE_CARD_SCORE <= 70 
	and 5 <= Local_HOUR <= 6
	and citi_tag = 1
then opp_class = "Citi Action";

else if ACTIMIZE_CARD_SCORE = 40 
    and Local_HOUR = 0
	and citi_tag = 1
then opp_class = "Citi Action";

else if ACTIMIZE_CARD_SCORE = 40 
	and 1 <= Local_HOUR <= 4
    and dol_amt <   800
	and citi_tag = 1
then opp_class = "Citi Action";

else if ACTIMIZE_CARD_SCORE = 40 
    and 5 <= Local_HOUR <= 6
	and citi_tag = 1
then opp_class = "Citi Action";        

else if ACTIMIZE_CARD_SCORE =30 
    and 1 <= Local_HOUR <= 6
	and citi_tag = 1
then opp_class = "Citi Action";

** swap - in citi *** ;
else if ACTIMIZE_CARD_SCORE = 20
    and 1 <= Local_HOUR <= 6
    and dol_amt >= 200
	and citi_tag = 1
then opp_class = "Swap_in_20_citi";

**************swap-out citi****************;
else if ACTIMIZE_CARD_SCORE = 40 
    and Local_HOUR  > 6
    and dol_amt <  200
	and citi_tag = 1
then opp_class = "Swap_out_citi";


** non-citi atm transactions ** ;
****** action segments for mexico ******;
else if ATM_CNTRY_CODE = "MX" 
		and 80 <= ACTIMIZE_CARD_SCORE <= 100
		and citi_tag = 0
		then opp_class = "Non-Citi MX_Action";

else if ATM_CNTRY_CODE = "MX" 
		and 50 <= ACTIMIZE_CARD_SCORE <= 70
		and citi_tag = 0
		then opp_class = "Non-Citi MX_Action";

else if ATM_CNTRY_CODE = "MX" 
		and ACTIMIZE_CARD_SCORE = 40
		and citi_tag = 0
		then opp_class = "Non-Citi MX_Action";

**** ATM_FIMP_Code action segment ***** ;
else if FIMP_CODE = "014" 
		and 80 <= ACTIMIZE_CARD_SCORE <= 100
		and citi_tag = 0 
		then opp_class = "Non-Citi IL_Action";

**** SWAP OUT segment ***** ;
else if  ACTIMIZE_CARD_SCORE = 40
		and citi_tag = 0 
		and Local_HOUR in (7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
		and dol_amt < 150
		then opp_class = "Non-Citi SwapOut_Med";

else if  50 <= ACTIMIZE_CARD_SCORE <=70 
		and citi_tag = 0 
		and Local_HOUR in (20,21,22,23)
		and dol_amt < 300
		then opp_class = "Non-Citi SwapOut_High";

else opp_class= "";

		
format CARD_CIN_NBR_2 $16.;
format Cust_type $32.;
CARD_CIN_NBR_2 = put(CIN_CARD_NBR,16.);

if substr(CARD_CIN_NBR_2,1,6)='517904' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='520159' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='528757' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536218' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536219' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536220' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536221' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536222' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536223' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536224' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536225' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536226' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536227' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536228' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='536229' then Cust_type="GOLD";
else if substr(CARD_CIN_NBR_2,1,6)='546527' then Cust_type="GOLD";
else Cust_type="Non-Gold";

format trigger $40.;
if ACTIMIZE_CARD_SCORE=40 then trigger="Medium risk";
else if  ACTIMIZE_CARD_SCORE=20 then trigger="Score=20";
else if ACTIMIZE_CARD_SCORE=30 then trigger="Score=30";
else if  ACTIMIZE_CARD_SCORE in (50,60,70) then trigger="High risk";
else if  ACTIMIZE_CARD_SCORE in (80,90,100) then trigger="V. High risk";
else trigger="Score=0,10";

*********************************************************************************************************;
*********************************************************************************************************;
*********************************************************************************************************;
*********************************************************************************************************;
*********************************************************************************************************;

******* creating card*day level figures *********************;
if Cust_type NE "GOLD" and	
   sum(of Sum_Same_day_withdrawal DOL_AMT)>=500 and
   fimp_code <> 0 and 
   highspenders_blue_flag =0
   then blue_grtr_500 = 1; else blue_grtr_500 = 0;

if Cust_type NE "GOLD" and	
   sum(of Sum_Same_day_withdrawal DOL_AMT)>=500 and
   fimp_code <> 0 and
   ACTIMIZE_CARD_SCORE >= 30 and
   highspenders_blue_flag =0
   then blue_grtr_500_scre_30_plus = 1; else blue_grtr_500_scre_30_plus   = 0;
   
***************************;
if Cust_type = "GOLD" and	
   sum(of Sum_Same_day_withdrawal DOL_AMT)>=1500 and
   highspenders_gold_flag =0
   then gold_grtr_1500 = 1; else gold_grtr_1500  = 0;
   
if Cust_type = "GOLD" and	
   sum(of Sum_Same_day_withdrawal DOL_AMT)>=1500 and
   highspenders_gold_flag =0 and 
    ACTIMIZE_CARD_SCORE >= 30 
   then gold_grtr_1500_scre_30_plus = 1; else gold_grtr_1500_scre_30_plus    = 0;  

******* creating card*day level figures *********************;
if Cust_type NE "GOLD" and	
   sum(of Sum_Same_day_withdrawal DOL_AMT)>=500 and
   fimp_code <> 0 and 
   highspenders_blue_flag =0 
   and fraud_tag_com=1
   then f_blue_grtr_500 = 1; else f_blue_grtr_500 = 0;

if Cust_type NE "GOLD" and	
   sum(of Sum_Same_day_withdrawal DOL_AMT)>=500 and
   fimp_code <> 0 and
   ACTIMIZE_CARD_SCORE >= 30 and
   highspenders_blue_flag =0
   and fraud_tag_com=1
   then f_blue_grtr_500_scre_30_plus = 1; else f_blue_grtr_500_scre_30_plus   = 0;
   
***************************;
if Cust_type = "GOLD" and	
   sum(of Sum_Same_day_withdrawal DOL_AMT)>=1500 and
   highspenders_gold_flag =0
   and fraud_tag_com=1
   then f_gold_grtr_1500 = 1; else f_gold_grtr_1500  = 0;
   
if Cust_type = "GOLD" and	
   sum(of Sum_Same_day_withdrawal DOL_AMT)>=1500 and
   highspenders_gold_flag =0 and 
    ACTIMIZE_CARD_SCORE >= 30 
	and fraud_tag_com=1
   then f_gold_grtr_1500_scre_30_plus = 1; else f_gold_grtr_1500_scre_30_plus    = 0;  
   
fraud_dol= DOL_AMT* fraud_tag_com;  

*******************************************************************************************************************************************;
********* 2. Missing value treatment of variables continued for created variables                                                 *********;
*******************************************************************************************************************************************;

** missing value treatment for last_trans_local_time: 45 days;
if dev_last_TRANS_LCL_DATE_TIME=. then dev_last_TRANS_LCL_DATE_TIME = 60*60*24*45; 

**average value for MOB is 3057 days (~8years) ;
if mob < 0 then mob= 3057;

*******************************************************************************************************************************************;
********* 3. BINning treatment of variables                                                                                       *********;
*******************************************************************************************************************************************;

format bn_Citi_ATM_Score $40.;
if EFD_CHAN_DESC = "Citi ATM" then do;
if  0<= Citi_ATM_Score <= 0.01 then bn_Citi_ATM_Score = "1. Top 1% ATMs";
else if Citi_ATM_Score <= 0.02 then bn_Citi_ATM_Score = "2. Top 2% ATMs";
else if Citi_ATM_Score <= 0.03 then bn_Citi_ATM_Score = "3. Top 3% ATMs";
else if Citi_ATM_Score <= 0.04 then bn_Citi_ATM_Score = "4. Top 4% ATMs";
else if Citi_ATM_Score <= 0.05 then bn_Citi_ATM_Score = "5. Top 5% ATMs";
else if Citi_ATM_Score <= 0.1  then bn_Citi_ATM_Score = "6. 5% - 10% ATMs";
else if Citi_ATM_Score <= 0.25 then bn_Citi_ATM_Score = "7. 10% - 25% ATMs";
else if Citi_ATM_Score <= 0.5  then bn_Citi_ATM_Score = "8. 25% - 50% ATMs";
else if 0.5 <= Citi_ATM_Score  <=1 then bn_Citi_ATM_Score = "9. Rest";
else bn_Citi_ATM_Score = "6. No Score";
end;

format bn_mob $40.;
if 0 <= mob < 15 then bn_mob="A. 15days";
else if 15 <= mob < 30 then bn_mob="B. 15-30days";
else if 30 <= mob < 60 then bn_mob="C. 30-60days";
else if 60 <= mob < 90 then bn_mob="D. 60-90days";
else if 90 <= mob < 180 then bn_mob="E. 90-180days";
else if 180 <= mob < 365 then bn_mob="F. 180-365days";
else if 365 <= mob < 365*5 then bn_mob="G. 1-5yrs";
else if 365*5 <= mob < 365*10 then bn_mob="H. 5-10yrs";
else if 365*10 <= mob < 365*15 then bn_mob="I. 10-15yrs";
else if mob >= 365*15 then bn_mob="J. 15+yrs";
else bn_mob = "ERROR";

format bn_dev_last_TRANS_LCL_DATE_TIME $20.;
if                      0  <=  dev_last_TRANS_LCL_DATE_TIME  <          60  then bn_dev_last_TRANS_LCL_DATE_TIME  = "A. <=1min";
else if                60  <=  dev_last_TRANS_LCL_DATE_TIME  <        60*3  then bn_dev_last_TRANS_LCL_DATE_TIME  = "B. 1 - 3 min";
else if              60*3  <=  dev_last_TRANS_LCL_DATE_TIME  <        60*5  then bn_dev_last_TRANS_LCL_DATE_TIME  = "C. 3 - 5 min";
else if              60*5  <=  dev_last_TRANS_LCL_DATE_TIME  <       60*10  then bn_dev_last_TRANS_LCL_DATE_TIME  = "D. 5 - 10 min";
else if             60*10  <=  dev_last_TRANS_LCL_DATE_TIME  <       60*15  then bn_dev_last_TRANS_LCL_DATE_TIME  = "E. 10 - 15 min";
else if             60*15  <=  dev_last_TRANS_LCL_DATE_TIME  <       60*60  then bn_dev_last_TRANS_LCL_DATE_TIME  = "F. 15 - 60 min";
else if             60*60  <=  dev_last_TRANS_LCL_DATE_TIME  <    60*60*24  then bn_dev_last_TRANS_LCL_DATE_TIME  = "G. 1 - 24 hrs";
else if          60*60*24  <=  dev_last_TRANS_LCL_DATE_TIME  <  60*60*24*5  then bn_dev_last_TRANS_LCL_DATE_TIME  = "H. 1 - 5 days";
else if        60*60*24*5  <=  dev_last_TRANS_LCL_DATE_TIME  < 60*60*24*30  then bn_dev_last_TRANS_LCL_DATE_TIME  = "I. 5 - 30 days";
else if       60*60*24*30  <=  dev_last_TRANS_LCL_DATE_TIME                 then bn_dev_last_TRANS_LCL_DATE_TIME  = "J. 30+ days";

format bn_localhr $40.;
if Local_HOUR  = 0 then bn_localhr = "A. 12AM - 1AM";
else if 1 <= Local_HOUR < 5 then bn_localhr = "B. 1AM - 5AM";
else if 5 <= Local_HOUR < 6 then bn_localhr = "C. 5AM - 6AM"; 
else if 6 <= Local_HOUR < 12 then bn_localhr = "D. 6AM - 12PM"; 
else if 12 <= Local_HOUR < 18 then bn_localhr = "E. 12PM - 18PM"; 
else if 18 <= Local_HOUR < 21 then bn_localhr = "F. 18PM - 21PM"; 
else if 21 <= Local_HOUR < 24 then bn_localhr = "G. 21PM - 12AM"; 

format Bn_dev_mean_score $40.;
if dev_mean_score<0 then Bn_dev_mean_score=           "A.   Increase<0";
else if dev_mean_score=0 then Bn_dev_mean_score=      "B.   NoChange";
else if dev_mean_score<=20 then Bn_dev_mean_score=     "C.   0< Increase <= 20";
else if dev_mean_score<=30 then Bn_dev_mean_score=     "D.   20< Increase <= 30";
else if dev_mean_score<=60 then Bn_dev_mean_score=     "E.   30< Increase <= 60";
else if dev_mean_score<=80 then Bn_dev_mean_score=     "F.   60< Increase <= 80";
else if dev_mean_score<=100 then Bn_dev_mean_score=     "G.   80< Increase <= 100";

*******************************************************************************************************************************************;
********* 5. Population selection flags                                                                                           *********;
*******************************************************************************************************************************************;

Flg_Phase_1_rules=  ((opp_class = "Citi Action")*1+(opp_class = "Citi Decline")*1)*1;   *** 1 if it is getting impacted by phase-1 decline or action rule *** ;
Flg_Limit_reductn_rules= (blue_grtr_500_scre_30_plus +gold_grtr_1500_scre_30_plus>=1)*1;  ** 1 if it is getting impacted by limit reduction *** ; 

*******************************************************************************************************************************************;
********* 6. Population inclusion flags                                                                                           *********;
*******************************************************************************************************************************************;

if ACTIMIZE_CARD_SCORE >=20;

run;

endsas;

proc freq data=crtdta.june_score_less_40;
tables Flg_Phase_1_rules /list missing;
run;

**%include '02_EDD_Macro_RA.sas';
**libname dd_apr '/cbna/cbnarisk/Essex/atm/analysis/edd/edd_sas';
**%edd(libname=crtdta, dsname=april_score20plus, edd_out_loc_xls=edd_apr_all_score_greater_than_20.xls , edd_out_loc_sas=dd_apr, NUM_UNIQ=Y,graphic=N, graph_loc=graphics);

data crtdta.june_score_less_40_an card_num (keep=CIN_CARD_NBR);
set crtdta.june_score_less_40;
ran=ranuni(111);
run;

proc sort data= card_num nodupkey;
by CIN_CARD_NBR;
run;

data card_num;
set card_num;
CIN_CARD_NBR_MSK=_n_;
run;

proc sort data=crtdta.june_score_less_40_an;
by CIN_CARD_NBR;
run;

data crtdta.june_score_less_40_an;
merge crtdta.june_score_less_40_an (in=a) card_num (in=b);
by CIN_CARD_NBR;
if a;
run;

proc sort data=crtdta.june_score_less_40_an;
by ran;
run;

data crtdta.june_score_less_40_an (drop = EFD_TRANS_ID CIN_CARD_NBR);
set crtdta.june_score_less_40_an;
EFD_TRANS_ID_MSK = _n_;
run;

proc sort data=crtdta.june_score_less_40_an nodupkey out=temp;
by EFD_TRANS_ID_MSK;
run;