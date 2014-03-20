options obs =0      errors = 2 mergenoby = error symbolgen macrogen; 

libname tom  '/cbna/cbnarisk/ts90692/data';
libname atm_in '/cbna/cbnarisk/Essex/atm/data';
%include '/cbna/cbnarisk/Essex/Teradata_logon/useridpwd.sas';
libname frd TERADATA user=&uid. password=&pwd.  tdpid='edwprod' database=P_CBNA_FR_ALL_V_C;
libname atmloc '/cbna/cbnarisk/Essex/atm/analysis/atm_location';
libname atm_perf '/cbna/cbnarisk/Essex/atm/data/atm_three_months_perf';
libname frd_tgs '/cbna/cbnarisk/Essex/atm/data/frd_tgs';

%LET date_begin = %sysfunc(mdy(7,1,2012)) ;
%LET date_end = %sysfunc(mdy(7,31,2012)) ;

** starting date : put one one month prior to starting date for 30days history ** ;
%LET rolling_date_begin = %sysfunc(mdy(6,1,2012)) ;
%LET prev_month_end = %sysfunc(mdy(6,30,2012)) ;

** starting date : put one one month prior to starting date for 90days history ** ;
%LET history_90_begin = %sysfunc(mdy(4,1,2012)) ;
%LET history_90_end = %sysfunc(mdy(6,30,2012)) ;

%LET perf_ds = july;

*********************************************************************************************************;
************************ 0. var creation from citi atm and non citi atm location risk variables *********;
*********************************************************************************************************;

 proc sql;
       create table atm_risk_var as
       select 
a.EFD_TRANS_ID        ,
a.TRANS_DATE          ,
a.EFD_CHAN_DESC       ,
a.ATM_REC_ID          ,
b.CIN_CARD_NBR                   
       from frd.trans_efd_atomic a, frd.trans_efd_detl_atomic b
       where a.efd_trans_id = b.efd_trans_id
       and a.trans_date GE &rolling_date_begin and a.trans_date LE &date_end
       and a.EFD_TRANS_TYPE in ('Cash Withdrawal')
	   and a.Auth_Decsn_code in ('APP');
   quit;
   
data june_onwards;
 infile '/cbna/cbnarisk/Essex/atm/create_data/fraud_Tags/*/*.csv' delimiter = ',' termstr=crlf MISSOVER DSD lrecl=32767 firstobs=1 ;
                informat EFD_TRANS_ID  $50. ;
				format EFD_TRANS_ID $50.;
              input EFD_TRANS_ID $;
run;

data app_system;
 infile '/cbna/cbnarisk/Essex/atm/create_data/fraud_tags_new/*.csv' delimiter = ',' termstr=crlf MISSOVER DSD lrecl=32767 firstobs=1 ;
                informat EFD_TRANS_ID  $50. ;
				format EFD_TRANS_ID $50.;
              input EFD_TRANS_ID $;
run;

*** pull fraud tags **** ;   
data fraud_Tags;
set app_system june_onwards frd_tgs.july_all_frd_tgs frd_tgs.june_all_frd_tgs frd_tgs.april_all_frd_tgs frd_tgs.may_all_frd_tgs;
run;

proc sort data=fraud_Tags (keep = EFD_TRANS_ID) nodupkey;
by EFD_TRANS_ID;
run;  

data curr_month previous_month;
if _n_ = 1 then do;
declare hash h(dataset: 'fraud_Tags');
        h.definekey('EFD_TRANS_ID');
        h.definedone();
    end;
set atm_risk_var;
if (h.find() = 0) then fraud_tag_com=1; else fraud_Tag_com = 0;
if &date_begin <=  trans_date <= &date_end then output curr_month;
if &rolling_date_begin <=  trans_date <= &prev_month_end then output previous_month;
run;

**qc**;
proc freq data=curr_month;
tables fraud_tag_com fraud_tag_com*trans_Date /list missing;
run;

*********************************************************************************************;
************ 0.1 var creation from citi atm location risk variables *************************;
*********************************************************************************************;

proc summary data=previous_month (where = (EFD_CHAN_DESC = "Citi ATM")) nway missing;
class ATM_REC_ID;
var fraud_tag_com;
output out = citi_atm_prev_mth sum=;
run;

data citi_atm_prev_mth;
set citi_atm_prev_mth ;
hit_Rate=fraud_tag_com/_freq_;
run;

proc sort data=citi_atm_prev_mth out=citi_atm_prev_mth;
by DESCENDING hit_Rate ; 
run;

** getting # ATMS in previous months **;
data citi_atm_prev_mth;
set citi_atm_prev_mth end = islast;
if islast=1 then do;
call symput('nobs',_n_);
end;
run;

%put &nobs.;

data citi_atm_prev_mth (keep = ATM_REC_ID Citi_ATM_Score);
set citi_atm_prev_mth;
Hit_Rate_Sno=_n_;
Citi_ATM_Score= Hit_Rate_Sno/&nobs.;
run;

*********************************************************************************************;
************ 0.2 var creation from non citi atm location risk variables *********************;
*********************************************************************************************;

proc summary data=previous_month (where = (EFD_CHAN_DESC NE "Citi ATM")) nway missing;
class ATM_REC_ID;
var fraud_tag_com;
output out = nciti_atm_prev_mth (rename = (fraud_tag_com= nonciti_fraud_count_atm) drop = _type_ _freq_) sum=;
run;

*********************************************************************************************;
************ 0.3 merging with original dataset **********************************************;
*********************************************************************************************;

data curr_month (keep = EFD_TRANS_ID Citi_ATM_Score nonciti_fraud_count_atm fraud_tag_com);
if _n_ = 1 then do; 
declare hash h1(dataset: 'citi_atm_prev_mth'); 
			h1.definekey('ATM_REC_ID'); 
			h1.definedata('Citi_ATM_Score'); 
			h1.definedone(); 
            call missing(Citi_ATM_Score);
declare hash h2(dataset: 'nciti_atm_prev_mth'); 
			 h2.definekey('ATM_REC_ID'); 
			 h2.definedata('nonciti_fraud_count_atm'); 
			 h2.definedone(); 
            call missing(nonciti_fraud_count_atm);		 
end;
set curr_month;
temp= h1.find()+h2.find();
run;

proc sort data=curr_month nodupkey;
by EFD_TRANS_ID;
run;

proc datasets lib=work;
delete nciti_atm_prev_mth previous_month citi_atm_prev_mth ;
quit;

*********************************************************************************************************;
************************ 1. Performance based variables *************************************************;
*********************************************************************************************************;

data all_Var;
set atm_perf.&perf_ds._rolling_variable_part1
	atm_perf.&perf_ds._rolling_variable_part2
	atm_perf.&perf_ds._rolling_variable_part3
	atm_perf.&perf_ds._rolling_variable_part4
	atm_perf.&perf_ds._rolling_variable_part5;
run;

proc sort data=  all_Var nodupkey;
by EFD_TRANS_ID;
run;

*********************************************************************************************************;
************************ 2. Rejected transactions variables**********************************************;
*********************************************************************************************************;

proc sort data= atm_perf.&perf_ds._rejects_all_var nodupkey out=rejects;
by EFD_TRANS_ID;
run;

*********************************************************************************************************;
************************ 3. High spenders distribution     **********************************************;
*********************************************************************************************************;

proc sql;
       create table atm_highspenders_all as
       select 
       a.EFD_TRANS_ID            ,
       a.ACCT_ID                 ,
       a.auth_Decsn_code         ,
	   a.FIMP_CODE              ,
	   a.trans_date				,
       a.CUST_ID                 ,
       a.DOL_AMT                 ,
       b.CIN_CARD_NBR                   
       from frd.trans_efd_atomic a, frd.trans_efd_detl_atomic b
       where a.efd_trans_id = b.efd_trans_id
       and a.trans_date GE &history_90_begin and a.trans_date LE &history_90_end
       and a.EFD_TRANS_TYPE in ('Cash Withdrawal')
	   and a.auth_Decsn_code in ('APP');
   quit;

data atm_highspenders_all;
set atm_highspenders_all;
      
format CARD_CIN_NBR_2 $16.;
format Cust_type $16.;
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
run;

************************ blue customers : done at card level ***************** ************************* ;

proc summary data=atm_highspenders_all (where = (Cust_type NE "GOLD")) nway missing;
class CIN_CARD_NBR Cust_type FIMP_CODE trans_date;
var DOL_AMT;
output out=card_cross_day_blue sum=;
run;

data card_cross_day_blue;
set card_cross_day_blue;
if  DOL_AMT >= 300 then dol_300_plus = 1; else dol_300_plus =0;
if  DOL_AMT >= 500 then dol_500_plus = 1; else dol_500_plus =0;
if  DOL_AMT >= 800 then dol_800_plus = 1; else dol_800_plus  =0;
if  DOL_AMT >= 1000 then dol_1000_plus = 1; else dol_1000_plus =0;
if  DOL_AMT >= 1500 then dol_1500_plus = 1; else dol_1500_plus =0;
if  DOL_AMT >= 2000 then dol_2000_plus = 1; else dol_2000_plus =0;
run;

proc summary data=card_cross_day_blue nway missing;
class CIN_CARD_NBR Cust_type;
var 		DOL_AMT
			dol_300_plus
			dol_500_plus
			dol_800_plus
			dol_1000_plus
			dol_1500_plus 
			dol_2000_plus ;
output out=card_level_blue sum=;
run;

************************ for blue customers putting the highspenders cut at ***************************** ;
**********************************daily withdrawal >= 500 and # trans>=1 *********************************;

data highspenders_blue (keep = CIN_CARD_NBR);
set card_level_blue;
** tking only blue cards ** ; 
if Cust_type NE "GOLD"; 
** taking only customers who have done >=1 transactions in the past three months **; 
if dol_500_plus>=1;
run;

proc sort data= highspenders_blue out=highspenders_blue;
by CIN_CARD_NBR;
run;

proc datasets lib=work;
delete card_cross_day_blue card_level_blue;
quit;

************************ gold customers : done at account level **************************************** ;

proc summary data=atm_highspenders_all (where = (Cust_type = "GOLD"))  nway missing;
class CIN_CARD_NBR Cust_type trans_date;
var DOL_AMT;
output out=card_cross_day_gold sum=;
run;

data card_cross_day_gold;
set card_cross_day_gold;
if  DOL_AMT >= 300 then dol_300_plus = 1; else dol_300_plus =0;
if  DOL_AMT >= 500 then dol_500_plus = 1; else dol_500_plus =0;
if  DOL_AMT >= 800 then dol_800_plus = 1; else dol_800_plus  =0;
if  DOL_AMT >= 1000 then dol_1000_plus = 1; else dol_1000_plus =0;
if  DOL_AMT >= 1500 then dol_1500_plus = 1; else dol_1500_plus =0;
if  DOL_AMT >= 2000 then dol_2000_plus = 1; else dol_2000_plus =0;
run;

proc summary data=card_cross_day_gold nway missing;
class CIN_CARD_NBR Cust_type;
var 		DOL_AMT
			dol_300_plus
			dol_500_plus
			dol_800_plus
			dol_1000_plus
			dol_1500_plus 
			dol_2000_plus ;
output out=card_level_gold sum=;
run;

************************ for gold customers putting the highspenders cut at ***************************** ;
**********************************daily withdrawal >= 1000 and # trans>=1 ********************************;

data highspenders_gold (keep = CIN_CARD_NBR);
set card_level_gold;
** tking only gold cards ** ; 
if Cust_type="GOLD"; 
** taking only customers who have done >=1 transactions in the past three months **; 
if dol_1500_plus>=1;
run;

proc sort data= highspenders_gold out=highspenders_gold;
by CIN_CARD_NBR;
run;

proc datasets lib=work;
delete card_cross_day_gold card_level_gold;
quit;

*********************************************************************************************************;
************************ 4. Max amnt withdrawal for past 90 days    *************************************;
*********************************************************************************************************;

proc summary data=atm_highspenders_all nway missing;
class CIN_CARD_NBR trans_date;
var DOL_AMT;
output out=atm_highspenders_all sum=;
run;

proc summary data=atm_highspenders_all nway missing;
class CIN_CARD_NBR;
var DOL_AMT;
output out=atm_highspenders_all (rename = (DOL_AMT= limit_withdrwl_max_90days) drop = _type_ _freq_) max=;
run;

*********************************************************************************************************;
************************ 5. Merging all datasets                    *************************************;
*********************************************************************************************************;

data atm_perf.&perf_ds._temp_Dataset;
merge all_Var   (in=a)
      rejects (in=b)
	  curr_month (in=c);
by EFD_TRANS_ID;
if a;
merge_indic= compress(a||b||c);
run;

proc datasets lib=work;
delete all_Var rejects curr_month;
quit;

proc sort data=atm_perf.&perf_ds._temp_Dataset;
by CIN_CARD_NBR;
run;

data atm_perf.&perf_ds._temp_Dataset;
merge atm_perf.&perf_ds._temp_Dataset (in=a) atm_highspenders_all (in=b) highspenders_blue (in=c) highspenders_gold (in=d);
by CIN_CARD_NBR;
if a;
if c then highspenders_blue_flag=1; else  highspenders_blue_flag =0;
if d then highspenders_gold_flag=1; else  highspenders_gold_flag =0;
merge_indic_2=compress(a||b||c||d);
run; 

proc datasets lib=work;
delete atm_highspenders_all;
quit;

proc freq data=atm_perf.&perf_ds._temp_Dataset;
tables merge_indic merge_indic_2 /list missing;
run;

proc sort data=atm_perf.&perf_ds._temp_Dataset nodupkey out=atm_perf.&perf_ds._temp_Dataset;
by EFD_TRANS_ID;
run;