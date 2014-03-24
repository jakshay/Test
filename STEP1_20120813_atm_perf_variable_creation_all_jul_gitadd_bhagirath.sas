options obs =max  errors = 2 mergenoby = error symbolgen macrogen; 

libname tom  '/cbna/cbnarisk/ts90692/data';
libname atm_in '/cbna/cbnarisk/Essex/atm/data';
libname atmloc '/cbna/cbnarisk/Essex/atm/analysis/atm_location';
%include '/cbna/cbnarisk/Essex/Teradata_logon/useridpwd.sas';
libname frd TERADATA user=&uid. password=&pwd.  tdpid='edwprod' database=P_CBNA_FR_ALL_V_C;
libname atm_perf '/cbna/cbnarisk/Essex/atm/data/atm_three_months_perf';

%LET date_begin = %sysfunc(mdy(7,1,2012)) ;
%LET date_end = %sysfunc(mdy(7,31,2012)) ;
** add comments *** ; 
** add comments *** ; 
* add comments *** ; 
** add comments *** ; 



** starting date : put one one month prior to starting date for 30days history ** ;
*******************************************************************************************************************************************;
********* 0. Pull main data and merging with ATM attributers                                                                      *********;
*******************************************************************************************************************************************;

 proc sql;
       create table all_data as
       select 
a.EFD_TRANS_ID        ,
a.Auth_Decsn_code     ,
a.TRANS_LCL_DATE_TIME ,
a.TRANS_DATE          ,
a.actimize_card_score ,
a.DOL_AMT             ,
a.ACCT_BAL_AMT        ,
a.fimp_code           ,
a.TRANS_BR_CODE       ,
a.EFD_CHAN_DESC       ,
a.ATM_REC_ID          ,
a.EFD_ACCT_OPEN_DT    ,
b.CIN_CARD_NBR                   
       from frd.trans_efd_atomic a, frd.trans_efd_detl_atomic b
       where a.efd_trans_id = b.efd_trans_id
       and a.trans_date GE &rolling_date_begin and a.trans_date LE &date_end
       and a.EFD_TRANS_TYPE in ('Cash Withdrawal')
	   and a.Auth_Decsn_code in ('APP');
   quit;

proc sort data=all_data out=all_data;
by ATM_REC_ID;
run;

proc sql;
create table atm_location_all as
select
t1.atm_rec_id,
t2.*
from 
(select
  a.atm_rec_id,
  count(efd_trans_id) as numtrx
  from frd.trans_efd_atomic a
  where a.trans_date ge &rolling_date_begin
  and a.EFD_TRANS_TYPE in ('Cash Withdrawal')
  group by a.atm_rec_id) as t1,
  frd.ATM_DEVICE t2
  where t1.atm_rec_id = t2.atm_rec_id
  ;
quit;  

proc sort data=atm_location_all nodupkey out=atm_location_all (keep = ATM_REC_ID ATM_CNTRY_CODE ATM_CITY ATM_ZIP_CODE ATM_ST_CODE);
by ATM_REC_ID;
run;

data two_months_perf all_july_data_all;
merge all_data (in=a) atm_location_all (in=b);
by ATM_REC_ID;
if a;
ranun = ranuni(1111111);
if &date_begin <=  trans_date <= &date_end then output all_july_data_all;
output two_months_perf;
run;

proc datasets lib=work;
delete atm_location_all all_data;
quit;

proc sort data=two_months_perf;
by CIN_CARD_NBR EFD_TRANS_ID TRANS_LCL_DATE_TIME ;
run;

**data all_trns_frd_crds2;
**if _n_ = 1 then do;
**declare hash h(dataset: 'hash_src');
**        h.definekey('CIN_CARD_NBR');
**        h.definedone();
**    end;
**set	 all_data2;
**if h.find() = 0 ;	
**run;

*******************************************************************************************************************************************;
********* 1. Initiating MACRO for performance variables create                                                                    *********;
*******************************************************************************************************************************************;

%macro dat_cre (ranun_begin, ranun_end, part_number);

data atm_july_all;
set all_july_data_all (where= ( &ranun_begin. <= ranun < &ranun_end.));
run;

proc sort data=atm_july_all nodupkey out=atm_july_all;
by efd_trans_id;
run;

***** keeping only cardnum and transaction id from the source ***;
data source_dataset;
set atm_july_all (keep = CIN_CARD_NBR EFD_TRANS_ID Auth_Decsn_code TRANS_LCL_DATE_TIME);
run;

proc sort data=source_dataset;
by CIN_CARD_NBR EFD_TRANS_ID TRANS_LCL_DATE_TIME;
run;

*** doing a left join in order to get "transaction history" for each transaction ***;
proc sql;
  create table all_perf as
  select 
  a.CIN_CARD_NBR as CIN_CARD_NBR_Src,
  a.EFD_TRANS_ID as EFD_TRANS_ID_Src,
  a.TRANS_LCL_DATE_TIME as TRANS_LCL_DATE_TIME_Src,
  b.* 
  from source_dataset a left join 
  two_months_perf b
  on  a.CIN_CARD_NBR = b.CIN_CARD_NBR
  where b.TRANS_LCL_DATE_TIME<=a.TRANS_LCL_DATE_TIME
  and datepart(a.TRANS_LCL_DATE_TIME)<=datepart(b.TRANS_LCL_DATE_TIME)+30
  ;
quit; 

data all_perf_excluding_curr;
set all_perf;

** all_perf_excluding_curr dataset contains all transactions in the past of that transaction **;
** it wont contain information abt the current transaction. it jst contains the past **;
if EFD_TRANS_ID NE EFD_TRANS_ID_Src;

********* master dataset creation done ********** now variables will  be created *****;

************************************************************************************;

************************************************************************************;
********** thirty days variable creation *******************************************;
************************************************************************************;

if EFD_CHAN_DESC = "Citi ATM" then citi_tag = 0;
else citi_tag = 1;

Local_HOUR =hour(TRANS_LCL_DATE_TIME); 

** creating variables based on local_hour  ** ;
if Local_HOUR  = 0 then trans_30_0 = 1; else trans_30_0 = 0;
if 1 <= Local_HOUR <= 4 then trans_30_1_4 = 1; else trans_30_1_4  = 0;
if 5 <= Local_HOUR <= 6 then trans_30_5_6 = 1; else trans_30_5_6  = 0;
if 0 <= Local_HOUR <= 6 then trans_30_0_6 = 1; else trans_30_0_6  = 0;
if Local_HOUR in (7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23) then trans_30_rest = 1; else trans_30_rest =0;

**qc_trans = trans_0 + trans_1_4  + trans_5_6 +trans_rest;

** creating variables based on dol_amnt  ** ;
if 0 <= dol_amt <= 200 then dol_30_200 = 1; else dol_30_200 =0;
if 200 < dol_amt <= 500 then dol_30_200_500 = 1; else dol_30_200_500   = 0;
if 500 < dol_amt <= 800 then dol_30_500_800 = 1; else dol_30_500_800 = 0;
if 800 < dol_amt then dol_30_800_plus = 1; else dol_30_800_plus  = 0;

if 500 < dol_amt then dol_30_500_plus = 1; else dol_30_500_plus   = 0;
if 200 < dol_amt <= 800 then dol_30_200_800 = 1; else dol_30_200_800  = 0;

**qc_dol =  dol_200 + dol_500 + dol_800 + dol_800_plus;

** creating variables based on on-us and off-us transactions *** ;
if EFD_CHAN_DESC = "Citi ATM" then on_us_30 = 1; else on_us_30 = 0;
if citi_tag = 1 then off_us_30 = 1; else off_us_30 = 0;
** qc_on_us  = on_us + off_us;

** # international transactions *** ;
if ATM_CNTRY_CODE NE "US" then intl_flag_30 =1; else intl_flag_30 =0;

**** # transactions in score bands ***********;
if actimize_card_score < 40 then score_30_untriggered = 1; else score_30_untriggered = 0;
if actimize_card_score >=40 then score_30_40_plus = 1; else score_30_40_plus = 0;
if actimize_card_score  = 40 then score_30_med = 1; else score_30_med = 0;
if 50 <= actimize_card_score <= 70  then score_30_high = 1; else score_30_high = 0;
if 80 <= actimize_card_score <= 100 then score_30_vhigh = 1; else score_30_vhigh  = 0;

************************************************************************************;
********** 24 hours transaction variable creation ********************************* ;
************************************************************************************;

if TRANS_LCL_DATE_TIME_Src<=TRANS_LCL_DATE_TIME+86400 then var_24=1 ; else var_24=0;

ACTIMIZE_CARD_SCORE_24 = ACTIMIZE_CARD_SCORE * var_24;
DOL_AMT_24 = DOL_AMT * var_24;

** creating variables based on local_hour  ** ;
if Local_HOUR  = 0 and var_24 =1 then trans_24_0 = 1; else trans_24_0 = 0;
if 1 <= Local_HOUR <= 4 and var_24 =1 then trans_24_1_4 = 1; else trans_24_1_4  = 0;
if 5 <= Local_HOUR <= 6 and var_24 =1 then trans_24_5_6 = 1; else trans_24_5_6  = 0;
if 0 <= Local_HOUR <= 6 and var_24 =1 then trans_24_0_6 = 1; else trans_24_0_6  = 0;
if Local_HOUR in (7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23) and var_24 =1 then trans_24_rest = 1; else trans_24_rest =0;

** qc_trans = trans_0 + trans_1_4  + trans_5_6 +trans_rest;

** creating variables based on dol_amnt  ** ;
if 0 <= dol_amt <= 200 and var_24 =1 then dol_24_200 = 1; else dol_24_200 =0;
if 200 < dol_amt <= 500 and var_24 =1 then dol_24_200_500 = 1; else dol_24_200_500   = 0;
if 500 < dol_amt <= 800 and var_24 =1 then dol_24_500_800 = 1; else dol_24_500_800 = 0;
if 800 < dol_amt and var_24 =1 then  dol_24_800_plus = 1; else dol_24_800_plus  = 0;

if 500 < dol_amt and var_24 =1 then dol_24_500_plus = 1; else dol_24_500_plus   = 0;
if 200 < dol_amt <= 800 and var_24 =1 then dol_24_200_800 = 1; else dol_24_200_800  = 0;

**qc_dol =  dol_200 + dol_500 + dol_800 + dol_800_plus;

** creating variables based on on-us and off-us transactions *** ;
if EFD_CHAN_DESC = "Citi ATM" and var_24 =1 then on_us_24 = 1; else on_us_24 = 0;
if citi_tag = 1 and var_24 =1 then off_us_24 = 1; else off_us_24 = 0;
** qc_on_us  = on_us + off_us;

if ATM_CNTRY_CODE NE "US" and var_24 =1 then intl_flag_24 =1; else intl_flag_24 =0;

**** # transactions in score bands ***********;
if actimize_card_score < 40 and var_24 =1 then  score_24_untriggered = 1; else score_24_untriggered = 0;
if actimize_card_score >=40 and var_24 =1 then  score_24_40_plus = 1; else score_24_40_plus = 0;
if actimize_card_score  = 40 and var_24 =1 then  score_24_med = 1; else score_24_med = 0;
if 50 <= actimize_card_score <= 70  and var_24 =1 then  score_24_high = 1; else score_24_high = 0;
if 80 <= actimize_card_score <= 100 and var_24 =1 then  score_24_vhigh = 1; else score_24_vhigh  = 0;

************************************************************************************;
********** five days transaction variable creation ********************************* ;
************************************************************************************;

if TRANS_LCL_DATE_TIME_Src<=TRANS_LCL_DATE_TIME+(86400*5) then var_5=1 ; else var_5=0;

ACTIMIZE_CARD_SCORE_5 = ACTIMIZE_CARD_SCORE * var_5;
DOL_AMT_5 = DOL_AMT * var_5;

** creating variables based on local_hour  ** ;
if Local_HOUR  = 0 and var_5 =1 then trans_5_0 = 1; else trans_5_0 = 0;
if 1 <= Local_HOUR <= 4 and var_5 =1 then trans_5_1_4 = 1; else trans_5_1_4  = 0;
if 5 <= Local_HOUR <= 6 and var_5 =1 then trans_5_5_6 = 1; else trans_5_5_6  = 0;
if 0 <= Local_HOUR <= 6 and var_5 =1 then trans_5_0_6 = 1; else trans_5_0_6  = 0;
if Local_HOUR in (7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23) and var_5 =1 then trans_5_rest = 1; else trans_5_rest =0;

** qc_trans = trans_0 + trans_1_4  + trans_5_6 +trans_rest;

** creating variables based on dol_amnt  ** ;
if 0 <= dol_amt <= 200 and var_5 =1 then dol_5_200 = 1; else dol_5_200 =0;
if 200 < dol_amt <= 500 and var_5 =1 then dol_5_200_500 = 1; else dol_5_200_500   = 0;
if 500 < dol_amt <= 800 and var_5 =1 then dol_5_500_800 = 1; else dol_5_500_800 = 0;
if 800 < dol_amt and var_5 =1 then  dol_5_800_plus = 1; else dol_5_800_plus  = 0;

if 500 < dol_amt and var_5 =1 then dol_5_500_plus = 1; else dol_5_500_plus   = 0;
if 200 < dol_amt <= 800 and var_5 =1 then dol_5_200_800 = 1; else dol_5_200_800  = 0;

**qc_dol =  dol_200 + dol_500 + dol_800 + dol_800_plus;

** creating variables based on on-us and off-us transactions *** ;
if EFD_CHAN_DESC = "Citi ATM" and var_5 =1 then on_us_5 = 1; else on_us_5 = 0;
if citi_tag = 1 and var_5 =1 then off_us_5 = 1; else off_us_5 = 0;
** qc_on_us  = on_us + off_us;

if ATM_CNTRY_CODE NE "US" and var_5 =1 then intl_flag_5 =1; else intl_flag_5 =0;

**** # transactions in score bands ***********;
if actimize_card_score < 40 and var_5 =1 then  score_5_untriggered = 1; else score_5_untriggered = 0;
if actimize_card_score >=40 and var_5 =1 then  score_5_40_plus = 1; else score_5_40_plus = 0;
if actimize_card_score  = 40 and var_5 =1 then  score_5_med = 1; else score_5_med = 0;
if 50 <= actimize_card_score <= 70  and var_5 =1 then  score_5_high = 1; else score_5_high = 0;
if 80 <= actimize_card_score <= 100 and var_5 =1 then  score_5_vhigh = 1; else score_5_vhigh  = 0;


************************************************************************************;
********** same calendar day withdrawal by customer ********************************;
************************************************************************************;

if datepart(TRANS_LCL_DATE_TIME_Src)=datepart(TRANS_LCL_DATE_TIME) then same_day_trans=1 ; else same_day_trans=0;
Sum_Same_day_withdrawal = DOL_AMT * same_day_trans;

run;

********************************************************************;
********************************************************************;
********************************************************************;
********************************************************************;
************taking summary of all the above created variables ******;
********************************************************************;
********************************************************************;
********************************************************************;
********************************************************************;
proc summary data= all_perf_excluding_curr nway missing;
class CIN_CARD_NBR_Src EFD_TRANS_ID_Src;
var 
DOL_AMT ACTIMIZE_CARD_SCORE 
trans_30_0 trans_30_1_4 trans_30_5_6 trans_30_0_6 trans_30_rest 
dol_30_200 dol_30_200_500 dol_30_500_800 dol_30_800_plus  dol_30_500_plus dol_30_200_800 
on_us_30 off_us_30 
intl_flag_30
score_30_untriggered score_30_40_plus score_30_med score_30_high score_30_vhigh 

var_24
ACTIMIZE_CARD_SCORE_24 DOL_AMT_24 
trans_24_0 trans_24_1_4 trans_24_5_6 trans_24_0_6 trans_24_rest 
dol_24_200 dol_24_200_500 dol_24_500_800 dol_24_800_plus dol_24_500_plus dol_24_200_800 
on_us_24 off_us_24 
intl_flag_24
score_24_untriggered score_24_40_plus score_24_med score_24_high score_24_vhigh 

var_5
ACTIMIZE_CARD_SCORE_5 DOL_AMT_5
trans_5_0 trans_5_1_4 trans_5_5_6 trans_5_0_6 trans_5_rest 
dol_5_200 dol_5_200_500 dol_5_500_800 dol_5_800_plus dol_5_500_plus dol_5_200_800 
on_us_5 off_us_5
intl_flag_5
score_5_untriggered score_5_40_plus score_5_med score_5_high score_5_vhigh

same_day_trans
Sum_Same_day_withdrawal
;
output out=perf_var_created (rename = (_freq_=Cnt_trans CIN_CARD_NBR_Src = CIN_CARD_NBR EFD_TRANS_ID_Src =EFD_TRANS_ID) drop=_type_)
max(DOL_AMT)=Max_DOL_AMT
mean(DOl_AMT)=Mean_DOL_AMT
max(ACTIMIZE_CARD_SCORE)=Max_ACTIMIZE_CARD_SCORE
mean(ACTIMIZE_CARD_SCORE)=Mean_ACTIMIZE_CARD_SCORE

sum(trans_30_0 )=trans_30_0 
sum(trans_30_1_4 )=trans_30_1_4 
sum(trans_30_5_6 )=trans_30_5_6 
sum(trans_30_0_6 )=trans_30_0_6 
sum(trans_30_rest)=trans_30_rest

sum(dol_30_200 )=dol_30_200 
sum(dol_30_200_500)=dol_30_200_500
sum(dol_30_500_800)=dol_30_500_800
sum(dol_30_800_plus )=dol_30_800_plus 
sum(dol_30_500_plus )=dol_30_500_plus 
sum(dol_30_200_800 )=dol_30_200_800 

sum(on_us_30 )=on_us_30 
sum(off_us_30 )=off_us_30 

sum(intl_flag_30)=intl_flag_30

sum(score_30_untriggered )=score_30_untriggered 
sum(score_30_40_plus )=score_30_40_plus 
sum(score_30_med )=score_30_med 
sum(score_30_high )=score_30_high 
sum(score_30_vhigh )=score_30_vhigh 

sum(var_24)=var_24
mean(ACTIMIZE_CARD_SCORE_24)=Mean_ACTIMIZE_CARD_SCORE_24
max(ACTIMIZE_CARD_SCORE_24)=Max_ACTIMIZE_CARD_SCORE_24
mean(DOL_AMT_24 )=mean_DOL_AMT_24 
max(DOL_AMT_24 )=max_DOL_AMT_24 
sum(DOL_AMT_24 )=sum_DOL_AMT_24 

sum(trans_24_0)=trans_24_0
sum(trans_24_1_4)=trans_24_1_4
sum(trans_24_5_6)=trans_24_5_6
sum(trans_24_0_6)=trans_24_0_6
sum(trans_24_rest)=trans_24_rest

sum(dol_24_200)=dol_24_200
sum(dol_24_200_500)=dol_24_200_500
sum(dol_24_500_800)=dol_24_500_800
sum(dol_24_800_plus)=dol_24_800_plus
sum(dol_24_500_plus)=dol_24_500_plus
sum(dol_24_200_800)=dol_24_200_800

sum(on_us_24)=on_us_24
sum(off_us_24)=off_us_24
sum(intl_flag_24)=intl_flag_24

sum(score_24_untriggered)=score_24_untriggered
sum(score_24_40_plus)=score_24_40_plus
sum(score_24_med)=score_24_med
sum(score_24_high)=score_24_high
sum(score_24_vhigh)=score_24_vhigh


sum(var_5)=var_5
mean(ACTIMIZE_CARD_SCORE_5)=mean_ACTIMIZE_CARD_SCORE_5
max(ACTIMIZE_CARD_SCORE_5)=max_ACTIMIZE_CARD_SCORE_5

mean(DOL_AMT_5)=mean_DOL_AMT_5
max(DOL_AMT_5)=max_DOL_AMT_5
sum(DOL_AMT_5)=sum_DOL_AMT_5

sum(trans_5_0)=trans_5_0
sum(trans_5_1_4)=trans_5_1_4
sum(trans_5_5_6)=trans_5_5_6
sum(trans_5_0_6)=trans_5_0_6
sum(trans_5_rest)=trans_5_rest

sum(dol_5_200)=dol_5_200
sum(dol_5_200_500)=dol_5_200_500
sum(dol_5_500_800)=dol_5_500_800
sum(dol_5_800_plus)=dol_5_800_plus
sum(dol_5_500_plus)=dol_5_500_plus
sum(dol_5_200_800)=dol_5_200_800

sum(on_us_5)=on_us_5
sum(off_us_5)=off_us_5

sum(intl_flag_5)=intl_flag_5

sum(score_5_untriggered)=score_5_untriggered
sum(score_5_40_plus)=score_5_40_plus
sum(score_5_med)=score_5_med
sum(score_5_high)=score_5_high
sum(score_5_vhigh)=score_5_vhigh

sum(same_day_trans         ) = same_day_trans
sum(Sum_Same_day_withdrawal) = Sum_Same_day_withdrawal
;
run;


******************************************************;
******* last transaction details *********************;
******************************************************;

proc sort data=all_perf_excluding_curr;
by EFD_TRANS_ID_Src TRANS_LCL_DATE_TIME;
run;

** taking only the last transaction ***;
data last_trans (rename=(CIN_CARD_NBR_Src = CIN_CARD_NBR EFD_TRANS_ID_Src =EFD_TRANS_ID));
set all_perf_excluding_curr ;
by EFD_TRANS_ID_Src;
if last.EFD_TRANS_ID_Src;

keep CIN_CARD_NBR_Src EFD_TRANS_ID_Src ACTIMIZE_CARD_SCORE DOL_AMT TRANS_LCL_DATE_TIME ACCT_BAL_AMT EFD_CHAN_DESC ATM_CNTRY_CODE ATM_CITY ATM_ZIP_CODE ATM_ST_CODE;
rename ACTIMIZE_CARD_SCORE = Last_ACTIMIZE_CARD_SCORE   ;
rename DOL_AMT = Last_DOL_AMT                                  ;
rename TRANS_LCL_DATE_TIME = last_TRANS_LCL_DATE_TIME          ;
rename ACCT_BAL_AMT = last_ACCT_BAL_AMT                        ;
rename EFD_CHAN_DESC = last_EFD_CHAN_DESC                      ;
rename ATM_CNTRY_CODE = last_ATM_CNTRY_CODE                    ;
rename ATM_CITY  = last_ATM_CITY                               ;
rename ATM_ZIP_CODE = last_ATM_ZIP_CODE                        ;
rename ATM_ST_CODE = last_ATM_ST_CODE                          ;
run;

*****************************************************************************************;
******* creating variable for max withdrawal of 30days for customer *********************;
*****************************************************************************************;
data all_perf_excluding_curr;
set all_perf_excluding_curr;
** deleting current day : and keeping only past days for the analysis ** ;
if datepart(TRANS_LCL_DATE_TIME_Src) = datepart(TRANS_LCL_DATE_TIME) then delete;
date = datepart(TRANS_LCL_DATE_TIME);
run;

proc summary data= all_perf_excluding_curr nway missing;
class CIN_CARD_NBR_Src EFD_TRANS_ID_Src date;
var DOL_AMT;
output out=date_history (rename = (_freq_=same_day_trans) drop=_type_)
sum(DOL_AMT)=DOL_AMT;
run;

proc summary data= date_history nway missing;
class CIN_CARD_NBR_Src EFD_TRANS_ID_Src ;
var DOL_AMT;
output out=withdrwl_30dys (rename=(CIN_CARD_NBR_Src = CIN_CARD_NBR EFD_TRANS_ID_Src =EFD_TRANS_ID) drop=_type_ _freq_)
max(DOL_AMT)=max_daily_withdrawal_30;
run;

******************************************************;
******* merging final created datasets ***************;
******************************************************;
proc sort data= perf_var_created;
by CIN_CARD_NBR EFD_TRANS_ID;
run;

proc sort data= atm_july_all;
by CIN_CARD_NBR EFD_TRANS_ID;
run;

proc sort data= last_trans;
by CIN_CARD_NBR EFD_TRANS_ID;
run;

proc sort data= withdrwl_30dys (keep = CIN_CARD_NBR EFD_TRANS_ID max_daily_withdrawal_30);
by CIN_CARD_NBR EFD_TRANS_ID;
run;

data atm_perf.july_rolling_variable_&part_number.;
merge atm_july_all (in=a) perf_var_created (in=b) last_trans (in=c) withdrwl_30dys (in=d);
by CIN_CARD_NBR EFD_TRANS_ID;
merge_indicator=compress(a||b||c||d);
run;

proc datasets lib=work;
delete atm_july_all perf_var_created last_trans summarised_trans_score date_history withdrwl_30dys;
quit;

%mend dat_cre;


%dat_cre(0,0.2,part1);
%dat_cre(0.2,0.4,part2);
%dat_cre(0.4,0.6,part3);
%dat_cre(0.6,0.8,part4);
%dat_cre(0.8,1.1,part5);