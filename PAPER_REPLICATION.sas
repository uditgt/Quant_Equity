******************************************************************************
PURPOSE		: 	Replicating results from 'Joint Cross Section of Stocks and 
				Options' by Ang A., Bali T.G., Cakici N. and An B.
INPUT		: 	Volatlity Surface data (read using Get_Vol_Data code),
				Fama-French factors (both daily and monthly freq.),
				CRSP (both daily and monthly freq.), Compustat fundamentals
OUTPUT		: 	Various tables and charts from the paper (refer to Step 3)
COMMENTS	:	Step 1 and 2 help prepare the dataset as required by the paper
				and Step 3 reproduces various results (shared in excel/ ppt)
AUTHOR		:	UDIT GUPTA (September, 2015)
*****************************************************************************

*****************************************************************************
PRELIMINARIES
****************************************************************************;
* Path to input folder;
libname DBfolder '\\dev-sasbi101\temp\Gupta\Datasets';

* Period for which data is to be read WITH sufficient buffer (+/- 3yr). 
  Includes both end points. Limited period helps with faster execution;
%let start_yr			= 1993;				*USER INPUT;
%let end_yr				= 2014;				*USER INPUT;
%let buffer				= 22;				*Any integer > length of time series in years;
%let Year_range			= &start_yr and &end_yr;	

* CRSP monthly file (RA's cleaned version. However, retains all SHRCD data);
%let raw_CRSP 			= crsp_201505all;	

* CRSP daily file (RA's cleaned version. Original available at \\irv-cvmedia02\research\Constituent Data\Daily CRSP\);
%let raw_CRSP_daily		= crsp_201501_daily;	

* Compustat data file (RA's cleaned version. However, retains DefTax variable);
%let raw_Compustat		= compustat_201505all;

* Fama French time series for factor returns (from K. French's website);
%let FF_Factors			= FF_factors_201505;	
%let FF_Factors_daily 	= ff_factors_daily_201412;	*Only till Dec 2014;

* Volatility_Surface (merged) dataset;
%let VS_data			= data_vs_merged;

*****************************************************************************
CODE OVERVIEW:
STEP 1: PREPARE DATASET (ADD VARIABLES) FOR CREATING TABLES FROM THE PAPER.
STEP 2: READ VOLATILITY DATESET AND MERGE PREDICTIVE VARIABLES TO IT.

		<<Copy of dataset saved as WorkDB_backup>>
STEP 3: REPLICATE RESULTS FROM THE PAPER.
****************************************************************************;


*****************************************************************************
STEP 1: PREPARE DATASET (ADD VARIABLES) FOR CREATING TABLES FROM THE PAPER.
1.1:  Read time series of factor returns (FF factors in this case)
1.2:  Read Risk-Free and Market daily returns (VWRETD from CRSP)
1.3:  Read Compustat Dataset

<<Using CRSP Monthly datafile>>
1.4:  Read CRSP Monthly Dataset
	  Create CRSP_dec Dataset
	  Create ILLIQ variable (based on Amihud 2002)
1.5:  Create MOM variable in CRSP Dataset
1.6:  Align Forward Period Return variables (next 6m) in CRSP Dataset

<<Using CRSP Daily datafile>>
1.7:  Read CRSP Daily data file
	  Calculate RVOL	(based on last 1mn daily data -> monthly freq.)
1.8:  Calculate BETA (based on last 1mn daily data -> monthly freq.)
1.9:  Prepare dummy variable needed to process SKEW & COSKEW			
1.10: Calculate COSKEW (based on last 1yr daily data -> monthly freq.)
1.11: Calculate SKEW (based on last 1yr daily data -> monthly freq.)
1.12: Update CRSP monthly file with RVOL, BETA, COSKEW and SKEW
****************************************************************************;

/*1.1: Read time series of factor returns (FF factors in this case)			*/
data FF_Factors;
	set DBfolder.&FF_factors;
	where Year between &Year_Range;
run;


/*1.2: Read Risk-Free and Market daily returns								*/
data FF_Factors_daily;
	set DBfolder.&FF_Factors_daily; 
	where Year between &Year_Range;	
run;

*Create Market excess return variable;
data FF_Factors_daily; 	set FF_Factors_daily;
	RmEx = VWRETD - Rf;	*The paper uses this instead of Mkt-Rf from FF;
run;

*Align previous day's value to present row;
proc sort data=FF_Factors_daily; by YEAR MONTH DAY; run;
data FF_Factors_daily; 	set FF_Factors_daily;
	by YEAR MONTH DAY;
	RmEx_Tm1 = lag(RmEx);	*Continuity in trading is assumed for VWRETD;
run;

*Align next day's value to present row;
proc sort data=FF_Factors_daily; by descending YEAR descending MONTH descending DAY; run;
data FF_Factors_daily; 	set FF_Factors_daily;
	by descending YEAR descending MONTH descending DAY;
	RmEx_Tp1 = lag(RmEx);	*Continuity in trading is assumed for VWRETD;
run;
proc sort data=FF_Factors_daily; by YEAR MONTH DAY; run;


/*1.3: Read Compustat Dataset												*/
data Compustat;
	set DBFolder.&raw_Compustat;
	where Year between &Year_range;	*Year represents Fiscal Year in Compustat dataset;
	PERMNO 	= WS_ID*1;		format PERMNO 11.;
	BE	=	sum(BV,defTax);		*Both are in $mn. As per Fama & French (1992);
	Data_year = year(FYE); 		*Data_year represents year in which data was released;
run;

		/*
		*ISSUE - Duplicate for PERMNO + Data_Year, due to corporate actions;
		proc sql;
			create table test as
			select Permno, Year, FYE, Data_Year, count(*) as ct
			from Compustat
			group by PERMNO, Data_Year
			having ct >1;

			create table test1 as
			select Permno, Year, FYE, Data_Year, BV
			from Compustat
			where PERMNO = 10515;
		quit;
		*/

*Keep only the latest obs for each Data_Year;
proc sort data=Compustat; 		by PERMNO Data_year ; run;
data Compustat; set Compustat; 	by PERMNO Data_year ; if last.Data_Year; run;


/*1.4: Read CRSP Monthly Dataset / Create CRSP_Dec / Create ILLIQ variable 	*/
data CRSP_monthly;	
	set DBfolder.&raw_CRSP; 
	where Year between &Year_range;
	DummyDt = mdy(month,1,year);

	Keep Permno Year Month Return Price MktCap AdjPrice Vol DummyDt;
	format return price vol 7.4 adjprice mktcap 7.2;
run;

*Volume is in Mn in RA's DB. 
 CRSP volume field is expressed in 100s for monthly data with -99 for missing.
 Hence RA's DB has Volume of -.0099 if the value is missing. 
 A volume of zero indicates that there were no trades during the time period 
 and is usually paired with bid/ask quotes in price fields.;
data CRSP_monthly;	set CRSP_monthly; 
	if vol = -.0099 then vol = .;
run;


data CRSP_Dec (drop=DummyDt);	
	set CRSP_Monthly;
	where month = 12;
	rename MktCap = MktCap_Dec;
run;


data CRSP_monthly;	set CRSP_monthly; 
	if AdjPrice>0 and Vol>0 
		then ILLIQ = abs(return)/(AdjPrice * Vol);
		else ILLIQ =.;
	format ILLIQ 7.4;
		*StockPrice is always non-negative (in RA's cleaned dataset);
		*Both Vol and AdjPrice are adjusted values;
run;


/*1.5: Create MOM variable in CRSP Dataset									*/
*Sort in Increasing order of Time;
proc sort data=CRSP_monthly; by Permno Year Month; run;


data CRSP_monthly; 	set CRSP_monthly;
	by Permno Year Month ;
	
	*Makes sure trading has been continuous (looking back);	
	month_gap_bck = intck('month', lag(DummyDt),DummyDt);* Datemonth - lag(month);

	retain line_bck;
	if first.Permno or (month_gap_bck ~= 1) 
		then line_bck = 1;
		else line_bck = line_bck + 1;
run;
		/*Following is a good example with gap in trading to see the above logic;
				proc sql;
					create table test as 
					select *
					from CRSP_monthly
					where PERMNO = 10346;
				run;
		*/

*Create Cummulative Return from -12 to -2 month;
data CRSP_monthly;	set CRSP_monthly; MOM = 0; format MOM 7.4; run;

%MACRO GetCummRet;
	%do iter = 1 %to 11;
	data CRSP_monthly;	set CRSP_monthly;
		by Permno Year Month;				
		MOM = (1+MOM)*(1+lag&iter(return))-1;
	run;
	%end;
%mend GetCummRet;
%GetCummRet;

*Set incorrectly calculated values to missing;
data CRSP_monthly;	set CRSP_monthly;
	if line_bck <12 then MOM = .;
run;


/*1.6: Align Forward Period Return variables (next 6m) in CRSP Dataset		*/
*We can have cases (e.g. if a stock gets suspended for a month) where
 ret_fwd1 is missing but ret_fwd2 is not;
proc sql;
	create table Temp_CRSP_monthly as 
	select t0.*, t1.return as ret_fwd1,		t2.return as ret_fwd2,		t3.return as ret_fwd3,
				 t4.return as ret_fwd4,		t5.return as ret_fwd5,		t6.return as ret_fwd6
	from CRSP_monthly as t0
		left join CRSP_monthly as t1	on t1.PERMNO = t0.PERMNO 	and intnx('month',t1.DummyDt,-1) = t0.DummyDt
		left join CRSP_monthly as t2	on t2.PERMNO = t0.PERMNO	and intnx('month',t2.DummyDt,-2) = t0.DummyDt
		left join CRSP_monthly as t3	on t3.PERMNO = t0.PERMNO	and intnx('month',t3.DummyDt,-3) = t0.DummyDt
		left join CRSP_monthly as t4	on t4.PERMNO = t0.PERMNO	and intnx('month',t4.DummyDt,-4) = t0.DummyDt
		left join CRSP_monthly as t5	on t5.PERMNO = t0.PERMNO	and intnx('month',t5.DummyDt,-5) = t0.DummyDt
		left join CRSP_monthly as t6	on t6.PERMNO = t0.PERMNO	and intnx('month',t6.DummyDt,-6) = t0.DummyDt
	order by t0.PERMNO, t0.DummyDt
;quit;

*Update;
data CRSP_monthly; set Temp_CRSP_monthly; run;
proc datasets noprint; delete Temp_CRSP_monthly; run;

		/*Following is a good example with gap in trading to see the above logic;
				proc sql;
					create table test as 
					select *
					from CRSP_monthly
					where PERMNO = 10346;
				run;
		*/

/*1.7: Read CRSP Daily data file / Calculate montly RVOL 					*/
*Read daily datafile;
data CRSP_Daily;
	retain PERMNO WS_ID YEAR MONTH DAY Return Price;
	set DBfolder.&raw_CRSP_daily;
	where YEAR between &Year_range;

	PERMNO = WS_ID*1;	format PERMNO 11.;
	format Return Price 7.4;
run;

proc sort data=CRSP_Daily; by PERMNO YEAR MONTH DAY; run;

*Calculate Realized volatility Table_RVOL (daily -> monthly freq.);
proc sql;
	create table Table_RVOL as 
	select Permno, Year, Month, STD(return) as RVOLd, count(*) as trade_days
	from CRSP_Daily
	group by Permno, Year, Month;
quit;
	*Comment:	The dataset includes obs where the stock stops trading 
	 during the month i.e. Trade_Days can be anything >=1. Ideally we 
	 should filter out Standard Deviation values calculated using less 
	 than a certain threshold number of days. However, in this case since 
     the paper doesn't mention it, we don't. These cases will get filtered out 
	 at portfolio formation stage, since there next month return is missing.

*Annualize RVOL;
data Table_RVOL;	set Table_RVOL;
	RVOL = RVOLd *sqrt(252);
	format RVOLd RVOL 7.4;
	DummyDt	= mdy(month,1,year);
run;

*Create RVOLdelta variable;
proc sql;
	create table Table_RVOL1 as
	select t1.*, (t1.RVOL - t2.RVOL) as RVOLdelta format=7.4
	from Table_RVOL as t1
	left join Table_RVOL as t2
		on  t1.PERMNO  = t2.PERMNO
		and t1.DummyDt = intnx('month',t2.DummyDt,1)
;quit;

*Align next months RVOL and RVOLDelta with each observation;
proc sql;
	create table Table_RVOL2 as
	select t1.*, t2.RVOL as RVOL_p1, t2.RVOLdelta as RVOLdelta_p1
	from Table_RVOL1 as t1
	left join Table_RVOL1 as t2
		on  t1.PERMNO  = t2.PERMNO
		and t1.DummyDt = intnx('month',t2.DummyDt,-1)
;quit;

*Update;
data Table_RVOL; set Table_RVOL2; run;
proc datasets noprint; delete Table_RVOL1 Table_RVOL2; run;



/*1.8: Calculate BETA (based on last 1mn daily data -> monthly freq.)		*/

*Merge Risk-free rate and Excess Market Return to CRSP daily datafile,
 for T-1 (Tm1), T (T0) and T+1 (Tp1) days, to estimate Beta adjusted for 
 non-synchronous trading [Dimson (1979)];
proc sql;
	create table CRSP_Rm_Daily as
	select t1.*, t2.Rf, t2.RmEx 	as RmEx_T0, 
					 	t2.RmEx_Tm1 as RmEx_Tm1, 
						t2.RmEx_Tp1 as RmEx_Tp1
	from CRSP_Daily as t1
		left join FF_Factors_Daily as t2
		on	t1.YEAR		=	t2.YEAR
		and	t1.MONTH	=	t2.MONTH
		and	t1.DAY		=	t2.DAY
	order by PERMNO, YEAR, MONTH, DAY
;
	title '1.8 Cases of missing RmEx (should be 0)!';
	select nmiss(RmEx_T0) as missing_RmEx
	from CRSP_Rm_daily
;
quit;

*Create Stock excess return variable (dependent variable in the regression);
data CRSP_Rm_daily;		set CRSP_Rm_daily;
	Return_excess = (Return - Rf);
run;

*Run monthly regressions to calculate Beta;
proc reg data= CRSP_Rm_daily
		 outest=Table_Beta (drop= _MODEL_ _TYPE_ _DEPVAR_ _RMSE_ Return_excess) noprint;
		 model Return_excess = RmEx_T0 RmEx_Tm1 RmEx_Tp1 	/noprint;	
		 by PERMNO YEAR MONTH;
run;
	*Comment - we get warnings in case of limited datapoints, or limited variation in 
			   data points;

*Calcualate overall BETA (sum of 3 partial Betas);
data Table_Beta;	set Table_Beta;
	BETA = RmEx_T0 + RmEx_Tm1 + RmEx_Tp1;

	rename	RmEx_T0		=	Beta_T0;
	rename	RmEx_Tm1	=	Beta_Tm1;
	rename	RmEx_Tp1	=	Beta_Tp1;
	drop intercept;
	format RmEX_: Beta 7.4;
run;

		/*
		*Beta = 0 when either the daily trade data shows '0' return and price,
		 or has too few observations to run a regression;
		proc sql;
			create table test as
			select *
			from Table_BETA
			where BETA = 0;

			create table test1 as
			select t1.*, t2.*
			from CRSP_Rm_daily as t1
			inner join test as t2
			on t1.PERMNO = t2.PERMNO
			and t1.YEAR = t2.YEAR and t1.MONTH = t2.MONTH;
		quit;
		*/

*Replace BETA = 0 with missing;
data Table_Beta;	set Table_Beta;
	if BETA = 0 then BETA = .;
	if Beta_T0 = 0 then Beta_T0 =.;
	if Beta_Tm1 = 0 then Beta_Tm1 =.;
	if Beta_Tp1 = 0 then Beta_Tp1 =.;
run;

*Calculate Alpha (monthly alphas are computed by summing the daily idiosyncratic returns 
 over the previous month);
proc sql;
	create table CRSP_Rm_daily_1 as 
	select t1.*, t2.Beta_T0, t2.Beta_Tm1, t2.Beta_Tp1
	from CRSP_Rm_daily as t1
	left join Table_Beta as t2
		on 	t1.PERMNO 	= 	t2.PERMNO
		and t1.Year		=	t2.Year
		and t1.Month	=	t2.Month
;quit;

data CRSP_Rm_daily_1; set CRSP_Rm_daily_1;
	Ret_idiosyncratic = Return_Excess - (RmEx_T0*Beta_T0 + RmEx_Tm1*Beta_Tm1 + RmEx_Tp1*Beta_Tp1);
	format Ret_idiosyncratic 7.4;
run;
	
proc sql;
	create table Table_Alpha as 
	select PERMNO, Year, Month, sum(Ret_idiosyncratic) as Alpha format=7.4
	from CRSP_Rm_daily_1
	group by PERMNO, Year, Month
;quit;

/*	data DBFolder.Table_Alpha; set Table_Alpha; run;	*/
proc datasets noprint; delete CRSP_Rm_daily_1; run;

/*1.9: Prepare dummy variable needed to process SKEW & COSKEW				*/	
%Macro Get_Port_Indicator(); 
/*
INPUT	:	3 Macro variables (&start_yr, &end_yr and &buffer)
OUTPUT	:	Port_Ind (Dataset containing unique value for each 12mn period)

PURPOSE	:
- We need to run monthly regressions using last 1yr of daily data.
- To be able to use 'BY' variable to run such regressions, we need a variable
  that uniquely identifies each 12month period.
- Since each observation will belong to 12 such 12mn periods, we will 
  identify these periods by creating variables P1 to P12.
- This macro will create such dummy variables for 12mn periods.
*/

	*Create dummy Indicator - unique value for each 12m period;
	data Port_Ind;
		date = mdy(1,1,&start_yr);
		do while (date < mdy(1,1,&end_yr+1));
		    output;
		    date = intnx('month', date, 1, 's');
		end;
		format date MMYY8.;
	run;

	data Port_Ind (drop= line); set Port_Ind;
		year = year (date);
		month = month(date);
		line = _N_;
		P1 = int( (line - 1 )/12)+1;
		P2 = lag(P1) + &buffer; 	P3 = lag(P2) + &buffer; 
		P4 = lag(P3) + &buffer; 	P5 = lag(P4) + &buffer; 
		P6 = lag(P5) + &buffer; 	P7 = lag(P6) + &buffer; 
		P8 = lag(P7) + &buffer; 	P9 = lag(P8) + &buffer; 
		P10 = lag(P9) + &buffer;	P11 = lag(P10) + &buffer; 
		P12 = lag(P11) + &buffer; 
	run;
%Mend; 

%Get_Port_Indicator();


*Merge Dummy values (P1 - P12) with CRSP_daily datafile;
proc sql;
	create table CRSP_RM_DAILY_short as 
	select 	t1.PERMNO, t1.Year, t1.Month, t1.Day, t1.Return, t1.Return_Excess,
			t1.RmEx_T0 as RmEx,
			t2.P1, 	t2.P2, 	t2.P3, 	t2.P4, 	t2.P5, 	t2.P6, 	t2.P7, 
			t2.P8, 	t2.P9, 	t2.P10, t2.P11, t2.P12
	from CRSP_RM_DAILY as t1
	left join Port_Ind as t2
		on 	t1.Year 	= t2.Year
		and t1.Month	= t2.Month
	order by Permno, Year, Month
;quit;


*Create a Table linking Port_Ind with its corresponding Year + Month (specific to PERMNO);
%Macro Get_Port_Ind_EndDt(inputdb = ); 
/*
INPUT			:	Merged database of CRSP_daily & Port_Ind created above
OUTPUT			:	Port_Ind_EndDt_clean
PURPOSE			:	Since a PERMNO can have data over any range of the Port_Ind period, 
					we will have cases, where due to lack of data, multiple Port_Ind end 
					in same Year + Month. This macro resolves such cases, and provide 
					a 1-to-1 linking between Port_Ind and Year + Month for each PERMNO.
*/
	%let flag_createdb  = 1;

	*Create a temporary dataset;
	data Temp_MacroDB;
		set &inputDB (keep= PERMNO YEAR MONTH P1-P12);
	run;		
	proc sort data= Temp_MacroDB nodupkey; by PERMNO YEAR MONTH; run;

	*Iterate from P1 - P12;
	%do iter = 1 %to 12;
		
		*Keep last obs (sorted on time) for each unique pair of PERMNO & P&Iter;
		proc sort data=Temp_MacroDB; by PERMNO P&Iter Year Month; run;

		data Temp_Iter;
			set Temp_MacroDB (keep=PERMNO P&Iter Year Month) ;
			by PERMNO P&Iter;
			rename P&Iter = Port_Ind;
			if last.PERMNO or last.P&Iter;
		run;

		*Append to a dataset;
		%if (&flag_createdb = 1 ) %then %do;
			data Port_Ind_EndDt;
				set Temp_Iter;
			run;
			%let flag_createdb = 0; 
		%end;
		%else %do;								
			/* data Year_Ind_EndDate;	set Year_Ind_EndDate Temp_Iter;	run; */
			proc append base= Port_Ind_EndDt data=Temp_Iter; run;		
		%end;
	%end;

	*Delete values which do not hold any meaning;
	data Port_Ind_EndDt; set Port_Ind_EndDt;
		where ~missing(Port_IND);
	run;

	*Remove cases where same PERMNO + YEAR + MONTH is tagged to multiple Indicators;
	proc sort data=Port_Ind_EndDt; by PERMNO YEAR MONTH; run;
	proc sql;
		create table Port_Ind_EndDt_clean as
		select PERMNO, YEAR, MONTH, min(Port_IND) as Port_Ind
		from Port_Ind_EndDt
		group by PERMNO, YEAR, MONTH
	;quit;

	*Delete temporary datasets;
	proc datasets noprint; delete Temp_MacroDB Temp_Iter Port_Ind_EndDt; run;
%Mend; 

%Get_Port_Ind_EndDt(inputdb = CRSP_RM_DAILY_short); 

/* *An example of how the logic will work for a stock which traded for 13 month;
		proc sql;
			create table test as 
			select *
			from CRSP_RM_DAILY_short
			where PERMNO = 10077;

			create table test1 as 
			select *
			from Port_Ind_EndDt_clean
			where PERMNO = 10077;
		quit;
*/


/*1.10: Create monthly COSKEW (based on last 1yr daily data -> monthly freq.)*/
*Create RmEx^2 variable (independent variable in regression);
data CRSP_RM_DAILY_short;	set CRSP_RM_DAILY_short;
	RmEx_sq = RmEx**2; 
	format Return Return_excess RmEx: 7.4;
run;


*Calculate COSKEW;
%Macro Cal_COSKEW( inputdb= );
/*
INPUT		:	InputDB (CRSP daily data file)
OUTPUT		:	Table_COSKEW (Dataset containing regression coefficient estimates)
PURPOSE		:	This is the primary Macro which calculates the monthly rolling regressions.
*/ 

	%let flag_createdb  = 1;

	%do iter = 1 %to 12;
	%put Loop: &iter;
		*Sort dataset;
		proc sort data=&inputDB; by PERMNO P&Iter; run;

		*Run monthly regressions with last 1yr data;
		proc reg data=&inputDB outest=Temp_COSKEW (drop= _MODEL_ _TYPE_ _DEPVAR_ _RMSE_ Return_excess
												  rename= (P&Iter = Port_Ind)) noprint;
				model Return_excess = RmEx RmEx_sq	/noprint;	
				by PERMNO P&Iter;
		run;
				*Comment - we get some warnings in case of limited datapoints, 
						   or limited variation in data points;

		*Append Results to a dataset;
		%if (&flag_createdb = 1 ) %then %do;
			data Table_COSKEW;	set Temp_COSKEW; run;
			%let flag_createdb = 0; 
		%end;
		%else %do;	
			proc append base= Table_COSKEW data=Temp_COSKEW; run;	
		%end;
	%end;

	*Delete values which do not hold any meaning;
	data Table_COSKEW;		set Table_COSKEW;
		where ~missing(Port_IND);
	run;

	*Delete temporary datasets;
	proc datasets noprint; delete Temp_COSKEW; run;
%Mend; 

%Cal_COSKEW( inputdb= CRSP_RM_DAILY_short);

*Correct 0 for missing;
data Table_COSKEW; set Table_COSKEW; 
	format intercept RmEx RmEx_sq 7.4;
	if RmEx_sq = 0 then RmEx_sq = .;
	if RmEx    = 0 then RmEx    = .;
run;


*Add YEAR and MONTH columns;
*COSKEW is the regression coefficient with RmEx_sq;
proc sql;
	create table Table_COSKEW1 as
	select t1.PERMNO, t2.Year, t2.Month, t1.Port_Ind, t1.RmEx_sq as COSKEW format=7.4
	from Table_COSKEW as t1
	left join Port_Ind_EndDt_clean as t2
		on 	t1.PERMNO	=	t2.PERMNO
		and	t1.Port_Ind = 	t2.Port_Ind
	order by PERMNO, YEAR, MONTH
;quit;

*Remove non-meaningful values;
data Table_COSKEW1; set Table_COSKEW1;
	where ~missing(Month);
run;

*Update;
data Table_COSKEW; set Table_COSKEW1; run;
proc datasets noprint; delete Table_COSKEW1; run;


/*1.11: Calculate SKEW (based on last 1yr daily data -> monthly freq.)		*/
*Calculate realized Skew;
%Macro Cal_SKEW( inputdb= );
/*
INPUT		:	InputDB (CRSP daily data file)
OUTPUT		:	Table_SKEW (Dataset containing monthly realised skew)
PURPOSE		:	This is the primary Macro which calculates the monthly rolling realised 
				skewness.
*/ 
	%let flag_createdb  = 1;

	%do iter = 1 %to 12;
	%put Loop: &iter;
		*Sort dataset;
		proc sort data=&inputDB; by PERMNO P&Iter; run;

		*Calculate monthly time series of realized Skewness in last 1yr;
		proc univariate data = &inputDB noprint;
			var Return ;
			by PERMNO P&Iter;
			output out = Temp_Skew (rename=(P&Iter = Port_Ind))
			skewness=Skew;	*VARDEF = DF by default; 
		run; 

		*Append Results to a table;
		%if (&flag_createdb = 1 ) %then %do;
			data Table_SKEW; set Temp_SKEW ; run;
			%let flag_createdb = 0; 
		%end;
		%else %do;	
			proc append base= Table_SKEW data=Temp_SKEW; run;
		%end;
	%end;

	*Delete values which do not hold any meaning;
	data Table_SKEW;	set Table_SKEW;
		where ~missing(Port_IND);
	run;

	*Delete temporary datasets;
	proc datasets noprint; delete Temp_SKEW; run;
%Mend; 

%Cal_SKEW( inputdb= CRSP_RM_DAILY_short);


*Add YEAR and MONTH columns;
proc sql;
	create table Table_SKEW1 as
	select t1.PERMNO, t2.Year, t2.Month, t1.Port_Ind, t1.SKEW format=7.4
	from Table_SKEW as t1
	left join Port_Ind_EndDt_clean as t2
		on 	t1.PERMNO	=	t2.PERMNO
		and t1.Port_Ind = 	t2.Port_Ind
	order by PERMNO, YEAR, MONTH
;quit;

*Remove non-meaningful values;
data Table_SKEW1; set Table_SKEW1;
	where ~missing(Month);
run;

*Update;
data Table_SKEW; set Table_SKEW1; run;
proc datasets noprint; delete Table_SKEW1; run;



/*1.12: Update CRSP monthly file with RVOL, BETA, ALPHA, COSKEW and SKEW		*/

proc sql;
	create table Temp_CRSP_Monthly as
	select t1.*, t2.RVOL, t2.RVOLdelta, t2.RVOL_p1, t2.RVOLdelta_p1, t2.trade_days,
				 t3.BETA, t4.ALPHA, t5.COSKEW, t6.SKEW
	from CRSP_Monthly  as t1

	left join Table_RVOL   as t2	on  t1.PERMNO	=	t2.PERMNO	and t1.YEAR		= 	t2.YEAR		and t1.MONTH	=	t2.MONTH
	left join Table_BETA   as t3	on  t1.PERMNO	=	t3.PERMNO	and t1.YEAR		= 	t3.YEAR		and t1.MONTH	=	t3.MONTH
	left join Table_ALPHA  as t4	on  t1.PERMNO	=	t4.PERMNO	and t1.YEAR		= 	t4.YEAR		and t1.MONTH	=	t4.MONTH
	left join Table_COSKEW as t5	on  t1.PERMNO	=	t5.PERMNO	and t1.YEAR		= 	t5.YEAR		and t1.MONTH	=	t5.MONTH
	left join Table_SKEW   as t6	on  t1.PERMNO	=	t6.PERMNO	and t1.YEAR		= 	t6.YEAR		and t1.MONTH	=	t6.MONTH

	order by PERMNO, YEAR, MONTH
;

	/*Show match ratios*/
	title '1.12 Cases of missing RVOL (need last 1m daily return data)';
	select nmiss(RVOL) as RVOL_missing, count(*) as NObs, 1-nmiss(RVOL)/count(*) as MatchRatio	from Temp_CRSP_Monthly;

	title '1.12 Cases of missing BETA (need last 1m daily return data)';
	select nmiss(BETA) as BETA_missing, count(*) as NObs, 1-nmiss(BETA)/count(*) as MatchRatio	from Temp_CRSP_Monthly;

	title '1.12 Cases of missing ALPHA (need last 1m daily return data)';
	select nmiss(ALPHA) as ALPHA_missing, count(*) as NObs, 1-nmiss(ALPHA)/count(*) as MatchRatio	from Temp_CRSP_Monthly;

	title '1.12 Cases of missing COSKEW (need last 1yr daily return data)';
	select nmiss(COSKEW) as COSKEW_missing, count(*) as NObs, 1-nmiss(COSKEW)/count(*) as MatchRatio	from Temp_CRSP_Monthly;

	title '1.12 Cases of missing SKEW (need last 1yr daily return data)';
	select nmiss(SKEW) as SKEW_missing, count(*) as NObs, count(*) - nmiss(SKEW) as Matched, 1-nmiss(SKEW)/count(*) as MatchRatio	from Temp_CRSP_Monthly;
quit;

	/*
	*Data exists in Monthly file, but not in Daily file!!
	data test;
		set temp_CRSP_monthly1;
		where missing(RVOL);
	run;

	data test1;
		set DBfolder.CRSP_201501_DAILY;
		where WS_ID = '10065';
	run;
		*Possible reason: upon cross refrencing with ShareCode data, it seems
		 very few of the PERMNOs with this issue have SHRCD 10/11. Most have 
		 other SHRCDs, with '73' being the largest;
	*/

*Update;
data CRSP_Monthly; set Temp_CRSP_Monthly; run;

*Delete intermediate datasets;
proc datasets noprint; 
	delete Temp_CRSP_Monthly CRSP_rm_daily_short Port_Ind Port_Ind_Enddt_clean
			;*Table_RVOL Table_Beta Table_Alpha Table_COSKEW Table_SKEW; 
run;



*****************************************************************************
STEP 2: READ VOLATILITY DATESET AND MERGE PREDICTIVE VARIABLES TO IT.
2.1: Read merged Volatility Dataset (with PERMNOs) in WORKDB
2.2: Align ImpVol for both Call & Put from +/- 6 months
2.3: Filter to retain stocks with matching PERMNO and within Paper period	
2.4: Add CRSP variables
		SIZE (end-of-Month MktCap), 
		MOM (long term 12-2m),
		REV (short term reversal from t-1 to t)
		ILLIQ (measure of illiquidity. higher value -> more illiquid)
		RVOL (realized volatility)
		RVOLdelta (monthly change in RVOL)
		BETA (market beta adjusted for non-synchronous trading)
		COSKEW (coskewness with market)
		SKEW (realized skewness)
		Forward 1m returns (upto next 6 months)
2.5: Add BM (book to market ratio)
2.6: Add OptionMetrics variables
		QSKEW (risk-neutral skew),
	 	IVOL (avg. ImpVol of atm call & put)
	 	RVOL_IVOL (spread between RVOL & IVOL)
****************************************************************************;

/*2.1: Read merged Volatility dataset (with PERMNOs) in WORKDB				*/
data WorkDB;	
	set DBfolder.&VS_data; 	
	format P50: P20: C50: PdCdDiff 7.4 CP_: 7.2; 
	DummyDt = mdy(month,1,year);
run;

/*2.2: Align ImpVol for both Call & Put from +/- 6 months					*/
*Align ImpVol from T to T+6;
proc sql;
	create table WorkDB1 as 
	select t0.*, t1.C50VOL as CV_p1,		t2.C50VOL as CV_p2,		t3.C50VOL as CV_p3,
				 t4.C50VOL as CV_p4,		t5.C50VOL as CV_p5,		t6.C50VOL as CV_p6,
				 t1.P50VOL as PV_p1,		t2.P50VOL as PV_p2,		t3.P50VOL as PV_p3,
				 t4.P50VOL as PV_p4,		t5.P50VOL as PV_p5,		t6.P50VOL as PV_p6
	from WorkDB as t0
		left join WorkDB as t1	on t1.SecurityID = t0.SecurityID 	and intnx('month',t1.DummyDt,-1) = t0.DummyDt
		left join WorkDB as t2	on t2.SecurityID = t0.SecurityID	and intnx('month',t2.DummyDt,-2) = t0.DummyDt
		left join WorkDB as t3	on t3.SecurityID = t0.SecurityID	and intnx('month',t3.DummyDt,-3) = t0.DummyDt
		left join WorkDB as t4	on t4.SecurityID = t0.SecurityID	and intnx('month',t4.DummyDt,-4) = t0.DummyDt
		left join WorkDB as t5	on t5.SecurityID = t0.SecurityID	and intnx('month',t5.DummyDt,-5) = t0.DummyDt
		left join WorkDB as t6	on t6.SecurityID = t0.SecurityID	and intnx('month',t6.DummyDt,-6) = t0.DummyDt
	order by t0.SecurityID, t0.DummyDt
;quit;
data WorkDB; set WorkDB1; run;

*Align ImpVol from T-6 to T;
proc sql;
	create table WorkDB1 as 
	select t0.*, t1.C50VOL as CV_m1,		t2.C50VOL as CV_m2,		t3.C50VOL as CV_m3,
				 t4.C50VOL as CV_m4,		t5.C50VOL as CV_m5,		t6.C50VOL as CV_m6,
				 t1.P50VOL as PV_m1,		t2.P50VOL as PV_m2,		t3.P50VOL as PV_m3,
				 t4.P50VOL as PV_m4,		t5.P50VOL as PV_m5,		t6.P50VOL as PV_m6
	from WorkDB as t0
		left join WorkDB as t1	on t1.SecurityID = t0.SecurityID 	and intnx('month',t1.DummyDt,+1) = t0.DummyDt
		left join WorkDB as t2	on t2.SecurityID = t0.SecurityID	and intnx('month',t2.DummyDt,+2) = t0.DummyDt
		left join WorkDB as t3	on t3.SecurityID = t0.SecurityID	and intnx('month',t3.DummyDt,+3) = t0.DummyDt
		left join WorkDB as t4	on t4.SecurityID = t0.SecurityID	and intnx('month',t4.DummyDt,+4) = t0.DummyDt
		left join WorkDB as t5	on t5.SecurityID = t0.SecurityID	and intnx('month',t5.DummyDt,+5) = t0.DummyDt
		left join WorkDB as t6	on t6.SecurityID = t0.SecurityID	and intnx('month',t6.DummyDt,+6) = t0.DummyDt
	order by t0.SecurityID, t0.DummyDt
;quit;
data WorkDB; set WorkDB1; run;

proc datasets noprint; delete WorkDB1; run;


/*2.3: Filter to retain stocks with matching PERMNO	and within Paper period	*/
data WorkDB; set WorkDB;
	where ~missing(PERMNO) and date < mdy(1,1,2012);
		*Since we need PERMNO to be able to join with CRSP/Compustat data,
		 read only those obs with non-missing PERMNOs. 
		 Limit data read to the period covered in the paper;
run;


/*2.4: Add SIZE, MOM, REV, ILLIQ, RVOL, BETA, COSKEW, SKEW, Forward Returns	*/
proc sql;
	create table WorkDB_1 as
	select 	t1.*, 			t2.MktCap, 		t2.MOM, 		t2.Return as REV, 	t2.ILLIQ, 		
			t2.trade_days,	t2.RVOL, 		t2.RVOLdelta, 	t2.RVOL_p1,			t2.RVOLdelta_p1,
			t2.BETA, 		t2.ALPHA,		t2.COSKEW, 		t2.SKEW,			t2.ret_fwd1, 		
			t2.ret_fwd2, 	t2.ret_fwd3, 	t2.ret_fwd4, 	t2.ret_fwd5, 		t2.ret_fwd6

	from WorkDB as t1
		left join CRSP_Monthly as t2
		on 	t1.PERMNO 	= 	t2.PERMNO
		and t1.Year		=	t2.Year
		and t1.Month	=	t2.Month
	order by PERMNO, YEAR, MONTH;

	title '2.1 Adding information from CRSP_monthly - missing details?';
	select 	nmiss(MktCap) as MktCap_missing, 	nmiss(MOM) as MOM_missing, 				
			nmiss(REV) as REV_missing, 			nmiss(ILLIQ) as ILLIQ_missing,
			nmiss(RVOL) as RVOL_mising, 		nmiss(RVOLdelta) as RVOLdelta_mising,  
			nmiss(BETA) as BETA_missing,		nmiss(ALPHA) as ALPHA_missing,
			nmiss(COSKEW) as COSKEW_missing, 	nmiss(SKEW) as SKEW_missing,
			nmiss(ret_fwd1) as Ret1_missing,	nmiss(ret_fwd2) as Ret2_missing,
			nmiss(ret_fwd3) as Ret3_missing,	nmiss(ret_fwd3) as Ret4_missing,
			nmiss(ret_fwd5) as Ret5_missing,	nmiss(ret_fwd4) as Ret6_missing
	from Workdb_1;	
	*Comment: There is a significant jump in missing cases from 1m to 2m, it suggests that RA's dataset captures 
	 delisting returns well. So the last obs in OM data for most securities, still has next month return;	
quit;

data WorkDB; 
	set WorkDB_1; 
	format Size 7.4;
	if MktCap = 0 then SIZE = .;
				  else SIZE = log(MktCap);	
				  *MCap = 0 are cases of Corporate actions, such as mergers;
run;

proc datasets noprint; delete WorkDB_1; run;


/*2.5: Add BM (book to market ratio)										*/
*Create PY variable to match as in the case of Fama-French 
 portfolio created in July;
data WorkDB;	set WorkDB;
	*Portfolio formation date;
	if month < 12 then do;	Nxt_month = month + 1;
							Nxt_year  = year;			end;
				  else do;	Nxt_month = 1;
							Nxt_year  = year + 1;		end;

	*Portfolios formed after 1st July will use last CY's data;
	if Nxt_month >= 7 	then PY = Nxt_year - 1;
						else PY = Nxt_year - 2;
run;


*Add PY Dec Market Cap;
proc sql;
	create table WorkDB_1 as
	select t1.*, t2.MktCap_Dec as ME
	from WorkDB as t1
		left join CRSP_dec as t2
		on 	t1.PERMNO 	= 	t2.PERMNO
		and t1.PY		=	t2.Year
	order by PERMNO, YEAR, MONTH
;

	title '2.2 Cases where ME (PY Dec MCap) is missing';
	select nmiss(ME) as missing
	from WorkDB_1
;		*Every stock entering the dataset between Jan - Dec will have this issue once;
quit;

*Add PY Book Equity (BE);
proc sql;
	create table WorkDB_2 as
	select t1.*, t2.BE
	from WorkDB_1 as t1
		left join Compustat as t2
		on 	t1.PERMNO	= 	t2.PERMNO
		and t1.PY		=	t2.Data_Year
	order by PERMNO, YEAR, MONTH;

	title '2.2 Cases where BE (from last CY) is missing';
	select nmiss(BE) as missing
	from WorkDB_2
;		*Every stock during its 1st year of listing will have this issue;
quit;

*Create BM = BE/ME variable;
data WorkDB_2; 	set WorkDB_2;

	*BM flag only requires value of Book Equity to be non-missing (negative are ok);
	if missing(BE) or ~(ME>0) 	then BMflag = .; 	else BMflag =1;	
	if ~(BE>0) or ~(ME>0)		then BM = .; 		else BM = BE/ME;
	format BM 7.4 BE 7.2;
run;

proc sql;
	title '2.2 Cases where BM (BE/ME) is missing (see comment)';
	select nmiss(BM) as missing
	from WorkDB_2
;quit;
	/* 
	Comment	: We find ~79k obs with missing BM.
	Reason 	: Most of the missing obs belong to the following ShrCds
				(1) Shrcd 11 (25k),
				 	- Of this only 15k have missing Book Equity (remaining have -ve values)
				 	- There represent ~1700 distinct PERMNOs
				 	- On analysing some of these cases, it turns out, the missing values usually occur
				   	  when a company lists (since as per FF method we use BE from previous calendar year) 
				(2)	Shrcd 31 for ADRs (25k), 
				(3)	Shrcd 73 for ETFs, Trusts, Index Funds etc.(24k)

	proc sql;
		create table test as
		select *
		from WorkDB_2
		where missing(BE) and shrcd in (10,11)
	;quit;

	proc sort data= test nodupkey; by PERMNO; run;

	proc sql;
		select *
		from Compustat
		where PERMNO in (81161,81169, 81875, 12872, 11896)
	;quit;
	*/
*Winzorise BM (monthly basis);
proc sort data=WorkDB_2; by Year Month; run;
proc univariate data = WorkDB_2 noprint;
	var BM ;
	by Year Month;
	output out = Winsorize_Limits
	pctlpts = 0.5 99.5 pctlpre = BM 
	NOBS=Obs N=Sample_Size NMISS=Missing_obs; 
		*Limits change during year, as sample_size changes;
		*While the #Obs change gradually from Dec to Jan, 
		 Sample size changes in jumps, which is expected dut to the 
		 nature of calculation of BM variable;
run; 

proc sql;
	create table WorkDB_3 as
	select t1.*, t2.BM0_5 as BMLow, t2.BM99_5 as BMHigh
	from WorkDB_2 as t1
	left join Winsorize_Limits as t2
		on t1.Year 	 = t2.Year
		and t1.Month = t2.Month;
quit;

data WorkDB_3;	set WorkDB_3;
	if ~missing(BM) then do;
		if      BM < BMLow 	then BM = BMLow;
		else if BM > BMHigh then BM = BMHigh;
	end;
	drop BMLow BMHigh;
run;

*Create Log(BM) variable from winsorized values;
data WorkDB_3; 	set WorkDB_3;	
	if missing(BM) 			then BMlog = .;
							else BMlog = log(BM);
	format BMlog 7.4;
run;

*Update;
data WorkDB;	set WorkDB_3;	run;
proc datasets noprint; delete WorkDB_: Winsorize_Limits; run;


/*2.6: Add QSKEW / Add IVOL  / Add RVOL_IVOL								*/
data WorkDB;	set WorkDB;
	QSKEW = P20VOL - 0.5*(P50VOL + C50VOL);
	IVOL  = 0.5*(P50VOL + C50VOL);
	RVOL_IVOL = RVOL - IVOL;
	format QSKEW IVOL RVOL_IVOL 7.4;
run;

*Backup;
data DBFolder.WorkDB_backup; 	set WorkDB;  run;
****************************************************************************;




*****************************************************************************
STEP 3: REPLICATE RESULTS FROM THE PAPER
3.0: Filter data to be used
3.1: Table I - Descriptive Statistics
3.2: Table II - Univariate Sorted Portfolios
3.3: Table III - Bivariate Sorted Portfolios
3.4: Table V - Long-term Predictability
3.5: Table IX - Forecasting Fama-Macbeth Regression
3.6: Figure I - Implied Volatilities Pre & Post Formation Months
3.7: Table X - Predicting Implied and Realized Volatilities
3.8: Table A - Checking if Mcap Refines The Signal?
3.9: Table IV - Descriptive Statistics For Bivariate Sorted Portfolios
3.10: Table C - Similar to Table V, but with incremental return only
****************************************************************************;

/*3.0: Filter data to be used												*/

*Reload WorkDB from Backup;
data WorkDB; set DBFolder.WorkDB_backup; run;		

*Filter WorkDB to retain valid obs;
data WorkDB;
	set WorkDB;
	if 	1
/*	(1) Predictive variables */
/*			and ~missing(BETA)  */
/*			and	~missing(SIZE)	*/
/*			and ~missing(BM)*/
/*			and ~missing(MOM) 	*/
/*			and ~missing(REV)	*/
/*			and ~missing(ILLIQ) */
/*			and ~missing(SKEW)	*/
/*			and ~missing(COSKEW)*/
/*			and ~missing(QSKEW)	*/
/*			and ~missing(BMflag)	In place of BM*/

	/*(2) Portfolio related variables */
			and	~missing(C50VOL) and ~missing(P50VOL)  
/*			and	~missing(C50VOLdelta) and ~missing(P50VOLdelta)  */
/*			and	~missing(ret_fwd1) */

	/*(3) Other relevant filters */
			and (SHRCD in (10,11))

	/*(4) Misc variables that can be used as filters */
/*			and	~missing(CP_volume) */
/*			and ~missing(CP_OI)*/
/*			and ~missing(RVOL) and ~missing(RVOLdelta) and ~missing(IVOL) and ~missing(RVOL_IVOL)*/
/*			and trade_days > 10*/
/*			and IssueType in ('0')*/

;run;

	/*
	*Looking at filtered data;
		proc sql;
		title 'by SHRCD(CRSP)';		select ShrCd, count(*) as count	from WorkDb	group by ShrCd;
		title 'by EXCHCD(CRSP)';	select EXCHCD, count(*) as count from WorkDb group by EXCHCD;
		title 'by IssueType (OM)';	select IssueType, count(*) as count	from WorkDb	group by IssueType;
		title 'by IndexFlag (OM)';	select IndexFlag, count(*) as count	from WorkDb	group by IndexFlag;
		quit;
	*/


*****************************************************************************
3.1: TABLE 1 - DESCRIPTIVE STATISTICS
- Average # of Stocks and Implied Volatility
- Average Firm Level Cross-correlation 
****************************************************************************;
data _Null_;run;
/*Average # of Stocks and Implied Volatility								*/
proc sql;	
	create table temp_Table1 as
	select Year, Month, count(distinct GVKEY) as ct
	from WorkDB
	group by Year, Month
	order by Year, Month;

	create table Table1a as
	select Year, Mean(ct) as Avg_Stocks format 7.0
	from temp_Table1
	group by Year
	order by Year;
quit;

proc sql;
	create table temp_Table1a as
	select YEAR, Mean(C50VOL) as AvgCVOL format=7.4,	Std(C50VOL) as StdCVOL format=7.4,
				 Mean(P50VOL) as AvgPVOL format=7.4,	Std(P50VOL) as StdPVOL format=7.4
	from WorkDB
	group by Year
	order by Year;

	create table Table1 as
	select t1.*, t2.AvgCVOL, t2.StdCVOL, t2.AvgPVOL, t2.StdPVOL
	from Table1a as t1
	inner join temp_Table1a	as  t2
		on t1.YEAR = t2.YEAR
	order by YEAR;
quit;

*Delete intermediata tables;
proc datasets noprint; delete Temp_Table1 Temp_Table1a Table1a; run;


/*Average Firm Level Cross-correlation 										*/
*Use securities with atleast 2 years of data;
proc sql;
	create table list_securityID as
	select SecurityID, count(*) as ct
	from WorkDB
	where (missing(P50VOL) + missing(C50VOL))<2
	group by SecurityID
	having ct >=24;
quit;

*Create subset of data for those SecurityIDs;
proc sql;
	create table data_Table1 as
	select t1.*
	from WorkDB as t1
	inner join list_securityID as t2
		on t1.SecurityId = t2.SecurityId
;quit;

*Calculate Firm-level cross correlation
 Note: Results seem closer when using Spearman rank correlation measure
 instead of Pearson's;
proc sort data=data_Table1; by SecurityID Year Month; run;
proc corr data = data_Table1 
		  out = Temp 	/* 	OUT  = Pearson correlation coeff.		
		  					OUTS = Spearman rank correlation coeff.	*/
		 (where= (_TYPE_ = 'CORR')) nomiss noprint;
	by SecurityID;
	var C50VOL P50VOL C50VOLdelta P50VOLdelta RVOL RVOLdelta RVOL_p1 RVOLdelta_p1;
run;

proc sql;
	create table Table1b as
	select _NAME_, mean(C50VOL) as C, 		mean(P50VOL) as P, 
				   mean(C50VOLdelta) as Cd, mean(P50VOLdelta) as Pd,
				   mean(RVOL) as RV, 		mean(RVOLdelta) as RVd,
				   mean(RVOL_p1) as RV_p1, 	mean(RVOLdelta_p1) as RVd_p1
	from Temp
	group by _NAME_
;quit;

data Table1b; set Table1b;
	if _NAME_ = 'C50VOL' 		then row = 1;	if _NAME_ = 'P50VOL' 		then row = 2;
	if _NAME_ = 'C50VOLdelta' 	then row = 3;	if _NAME_ = 'P50VOLdelta' 	then row = 4;
	if _NAME_ = 'RVOL' 			then row = 5;	if _NAME_ = 'RVOLdelta' 	then row = 6;
	if _NAME_ = 'RVOL_p1' 		then row = 7;	if _NAME_ = 'RVOLdelta_p1' 	then row = 8;
	format _numeric_ 7.4;
run;
proc sort data=Table1b out=Table1b (drop=row); by row;run;

*Delete intermediate datasets;
proc datasets noprint; delete list_securityID data_Table1 Temp; run;


*****************************************************************************
3.2: TABLE II - UNIVARIATE SORTED PORTFOLIOS
- Remove obs with missing next month return							
- INDEPENDENT sorting of stocks each month
- Macro to create results as per Table 2 from the paper	
****************************************************************************;
data _Null_;run;
/*Remove obs with missing next month return	(not available to trade)		*/
data WorkDB_1;
	set WorkDB;
	where ~missing(ret_fwd1);
run;

/*INDEPENDENT sorting of stocks each month									*/
*Rank;
proc sort  data=WorkDB_1; by Year Month; run;
proc rank data= WorkDB_1 out=WorkDB_1 groups = 10;
	var C50VOLdelta P50VOLdelta PdCdDiff;
	by Year Month;
	ranks C50VOLD_rk P50VOLD_rk PdCdDiff_rk;
run;


/*Macro to create results as per Table 2 from the paper						*/
%Macro Cal_Ret_TS_EW (DB = , Rank = , FactorTS = , High = , Low = , MCap = , Lag = );
/*
INPUT (definition of variables):
DB 			= Name of dataset 
Rank 		= Name of rank variable (used for independent sort)
FactorTS 	= Name of dataset containing factor returns
High 		= Rank 'value' of highest portfolio
Low 		= Rank 'value' of lower portfolio
Mcap		= Changes the portfolios from Equaly-Weighted to Mcap-weighted
Lag 		= Lag for computing Newey-West corrected tStat (Only for Long Short Portfolio)

OUTPUT: 	Table2_&Rank

PURPOSE:
- Replicate Table 2 from the paper. Generates avg. return in sorted decile portfolios.
- This macro calculates Avg. 1 Month holding return (Price + Distributions) 
  on a portfolio which is rebalanced Monthly depending on ranks (Independent Sorts).
- Portfolios are Equally Weighted and NOT Mcap weighted.
- The macro regresses all Portfolios with Factor time series
- In the present macro we have included three models: (1) CAPM (2) FF3, and (3) FF5
- The model also calculates tStat using Newey-West correction, since error terms are
  auto-correlated. 
*/

/* For debugging
%let db = WorkDB_1;		%let rank = C50VOLD_rk;		%let FactorTS = FF_Factors;
%let High = 9;			%let Low = 0;				%let MCap = No; 			%let Lag = 6;
*/

*Calculate Equal Weights for monthly portfolios (exclude stocks with missing ranks);
proc sql;
	create table Temp_Weights as
	select Nxt_Year, Nxt_Month, &Rank, 1/count(*) as Wt, count(*) as Count, sum(MktCap) as SumMcap
	from &DB
	where ~missing(&Rank)
	group by Nxt_Year, Nxt_Month, &Rank
;quit;

*Merge weights with return dataset (INNER JOIN);
proc sql;
	create table Temp_MacroDB as
	select t1.*, t2.Wt, t2.SumMcap
	from &DB as t1
		inner join Temp_Weights as t2
		on  t1.&Rank		=	t2.&Rank
		and t1.Nxt_Year		=	t2.Nxt_Year
		and t1.Nxt_Month	=	t2.Nxt_Month
;quit;

*Overwrite Stock weights to be Market Weighted;
%if %upcase(&Mcap) = YES
	%then %do;
		data Temp_MacroDB;	set Temp_MacroDB;
			Wt = MktCap/SumMCap;
		run;
	%end;

*Overwrite Stock weights to be Inverse Market Weighted;
%if %upcase(&Mcap) = INV
	%then %do;
		data Temp_MacroDB;	set Temp_MacroDB;
			Wt = SumMCap/MktCap;
		run;

		*Normalize Weights;
		proc sql;
			create table Temp_SumWt as
			select Nxt_Year, Nxt_Month, &Rank, sum(Wt) as SumWt
			from Temp_MacroDB 
			group by Nxt_Year, Nxt_Month, &Rank;
		quit;

		proc sql;
			create table Temp_MacroDB1 as
			select t1.*, t2.SumWt
			from Temp_MacroDB as t1
			left join Temp_SumWt as t2
				on  t1.&Rank		=	t2.&Rank
				and t1.Nxt_Year		=	t2.Nxt_Year
				and t1.Nxt_Month	=	t2.Nxt_Month
		;quit;

		data Temp_MacroDB; 
			set Temp_MacroDB1;
			Wt = Wt/SumWt;
		run;
	%end;

*Calculate Returns Time Series;
proc sql;
	create table Ret_TS as
	select Nxt_Year, Nxt_Month, &Rank, 
		   count(Wt) as Stocks, 
		   sum(ret_fwd1*Wt) as PortRet format=7.4
	from Temp_macroDB
	group by Nxt_Year, Nxt_Month, &Rank
	order by Nxt_Year, Nxt_Month, &Rank;
quit;

*Calculate Long Short portfolio time series;
proc sql;
	create table Temp_LSport as 
	select t1.Nxt_Year, t1.Nxt_Month, &high&Low as &Rank, (t1.PortRet - t2.PortRet) as PortRet
	from Ret_TS as t1
		inner join Ret_TS as t2
		on 	t1.Nxt_Year	 =	t2.Nxt_Year
		and t1.Nxt_Month =	t2.Nxt_Month
		and t1.&Rank = &High
		and t2.&Rank = &Low;
quit;

*Add Long-Short portfolio TS to RET_TS;
data Ret_TS; set Ret_TS temp_LSport; run;

*Add Factor Return Time Series; 
proc sql;
	create table Ret_Factors_TS as
	select t1.*, t2.Rf, t2.MKT_Rf, t2.SMB3, t2.HML, t2.SMB5, t2.RMW, t2.CMA, t2.MOM
	from Ret_TS	as t1
		left join &FactorTS as t2
		on  t1.Nxt_Year		=	t2.Year
		and t1.Nxt_Month	=	t2.Month
	order by &Rank, Year, Month;
quit;

*Calculate Excess Returns;
data Ret_Factors_TS;	set Ret_Factors_TS;
	if &Rank = &high&low then PortRetEx = PortRet;
						 else PortRetEx = PortRet - Rf;
run;

*Mean of LongShort Portfolio - WITH Newey-West correction;
proc model data=Ret_Factors_TS (where=(&Rank = &high&low));
title 'Mean of Long-Short Portfolio';
	endo PortRetEx;
	instruments / intonly;
	parms b0;
	PortRetEx=b0;
	fit PortRetEx / gmm kernel=(bart,%eval(&Lag+1),0) vardef=n dw=10 dwprob;
	ods output ParameterEstimates=Temp_Mean;
run; 
quit;

*Keep only the tValue;	
data Temp_Mean;		set Temp_Mean;
	drop Parameter EstType probt df Estimate StdErr;

	*Change format to make it compatible with Table2a_temp;
	&Rank = &High&Low; format &Rank 8.;
	Type = 'tValue';
	Rename tValue = AvgRet;
run;

*Mean Return of other decile portfolios (Ret_TS contains raw returns);
proc sql;
	create table Table2a_temp as 
	select &Rank, 'Estimate' as Type, Mean(PortRet) as AvgRet
	from Ret_TS
	group by &Rank;
quit;

data Table2a_temp;
	set Table2a_temp Temp_Mean;
	format AvgRet 7.4;
run;


*Regression using PROC MODEL - implements Newey-West correction;
*Calculate Newey-West T statistic for Model 1, 2, 3 & 4;
proc model data=Ret_Factors_TS ;*(where=(&Rank in(&Low, &High, &High&Low)));
title 'CAPM';
		parms alpha b1;
		by &Rank;
		PortRetEx = alpha + b1*Mkt_Rf;
        fit PortRetEx / gmm kernel=(bart,%eval(&Lag+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=temp_model1;
run; 
quit;
proc model data=Ret_Factors_TS ;*(where=(&Rank in(&Low, &High, &High&Low)));
title 'FF3';
		parms alpha b1 b2 b3;
		by &Rank;
		PortRetEx = alpha + b1*Mkt_Rf + b2*SMB3 + b3*HML;
        fit PortRetEx / gmm kernel=(bart,%eval(&Lag+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=temp_model2;
run; 
quit;
proc model data=Ret_Factors_TS ;*(where=(&Rank in(&Low, &High, &High&Low)));
title 'FF5';
		parms alpha b1 b2 b3 b4 b5;
		by &Rank;
		PortRetEx = alpha + b1*Mkt_Rf + b2*SMB5 + b3*HML + b4*RMW + b5*CMA;
        fit PortRetEx / gmm kernel=(bart,%eval(&Lag+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=temp_model3;
run; 
quit;
proc model data=Ret_Factors_TS ;*(where=(&Rank in(&Low, &High, &High&Low)));
title 'FF5_MOM';
		parms alpha b1 b2 b3 b4 b5 b6;
		by &Rank;
		PortRetEx = alpha + b1*Mkt_Rf + b2*SMB5 + b3*HML + b4*RMW + b5*CMA + b6*MOM;
        fit PortRetEx / gmm kernel=(bart,%eval(&Lag+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=temp_model4;
run; 
quit;

*Calculate columns for 'alpha' from different models
- Combine results from all 3 models
- Transpose to wide format from long format
- Cosmetic changes to match the output from paper;
proc sql;
create table Table2b_temp as 
	select *, 'CAPM' as Model
	from temp_model1
		UNION CORRESPONDING ALL
		select *, 'FF3' as Model
		from temp_model2
			UNION CORRESPONDING ALL
			select *, 'FF5' as Model
			from temp_model3
				UNION CORRESPONDING ALL
				select *, 'FF5_MOM' as Model
				from temp_model4
;quit;

data Table2b_temp; set Table2b_temp; 
	where Parameter = 'alpha'; 
	format Estimate StdErr tValue 7.4;
	drop Parameter EstType probt df;	
run;

proc sort data=Table2b_temp; by &Rank; run;
proc transpose data=Table2b_temp  out=Table2b_temp (drop=_LABEL_)  ;
	by &Rank;
	id Model;
	var Estimate tValue;
run;

data Table2b_temp; set Table2b_temp; *Display t-stat only for the Long Short portfolio;
	if ( _NAME_ = 'tValue' ) and ( &Rank ne &High&Low ) then delete;
run;

*Merge all columns together;
proc sql;
	create table Table2_&Rank as
	select t1.&Rank as Portfolio, t1._NAME_ as Type, t2.AvgRet, t1.CAPM, t1.FF3, t1.FF5, t1.FF5_MOM
	from Table2b_temp as t1
	left join Table2a_temp as t2
		on t1.&Rank   = t2.&Rank
		and t1._NAME_ = t2.Type;
quit;

*Delete intermediate datasets;
proc datasets noprint; 
	delete Temp_:	Ret_:	table2a_temp table2b_temp; 
run;
%mend Cal_Ret_TS_EW;

*Lag values can be confirmed by looking at Durbin-Watson statistic on the results screen for Portfolio '90'. 
 All three models give the same lag value. Significance level is ~98% in all the following three cases.;
*Call and Put-Call shows positive autocorrelation, while Put shows negative correlation;

*EQUAL_WEIGHTED;
%Cal_Ret_TS_EW(db = WorkDB_1, rank = C50VOLD_rk	, FactorTS = FF_Factors, High = 9, Low = 0, MCap = No, Lag = 6)
%Cal_Ret_TS_EW(db = WorkDB_1, rank = P50VOLD_rk	, FactorTS = FF_Factors, High = 9, Low = 0, MCap = No, Lag = 3)
%Cal_Ret_TS_EW(db = WorkDB_1, rank = PdCdDiff_rk, FactorTS = FF_Factors, High = 9, Low = 0, MCap = No, Lag = 6)

*MCAP_WEIGHTED;
%Cal_Ret_TS_EW(db = WorkDB_1, rank = C50VOLD_rk	, FactorTS = FF_Factors, High = 9, Low = 0, MCap = Yes, Lag = 0)
%Cal_Ret_TS_EW(db = WorkDB_1, rank = P50VOLD_rk	, FactorTS = FF_Factors, High = 9, Low = 0, MCap = Yes, Lag = 3)
%Cal_Ret_TS_EW(db = WorkDB_1, rank = PdCdDiff_rk, FactorTS = FF_Factors, High = 9, Low = 0, MCap = Yes, Lag = 0)

*INVERSE_MCAP_WEIGHTED;
%Cal_Ret_TS_EW(db = WorkDB_1, rank = C50VOLD_rk	, FactorTS = FF_Factors, High = 9, Low = 0, MCap = Inv, Lag = 0)
%Cal_Ret_TS_EW(db = WorkDB_1, rank = P50VOLD_rk	, FactorTS = FF_Factors, High = 9, Low = 0, MCap = Inv, Lag = 0)
%Cal_Ret_TS_EW(db = WorkDB_1, rank = PdCdDiff_rk, FactorTS = FF_Factors, High = 9, Low = 0, MCap = Inv, Lag = 8)


*Delete intermediate dataset;
proc datasets noprint; delete WorkDB_1; run;


*****************************************************************************
3.3: TABLE III - BIVARIATE SORTED PORTFOLIOS
- Remove obs with missing next month return							
- Double Sort the stocks each month (DEPENDENT SORT)
- Macro to create results as per Table 3 of the paper
****************************************************************************;
data _Null_;run;
%Macro Get_WorkDB2(db=);
	*Same as WorkDB_2 created earlier;
	/*Remove obs with missing next month return									*/
	data WorkDB_2;
		set WorkDB;
		where ~missing(ret_fwd1);
	run;

	/*Double Sort the stocks each month (DEPENDENT SORT)						*/
	*Create control rankings (independent of each other);
	proc sort  data=WorkDB_2; by Year Month; run;
	proc rank data= WorkDB_2 out= WorkDB_2 groups = 10;
		var C50VOLdelta P50VOLdelta;
		by Year Month;
		ranks C50VOLD_control P50VOLD_control;
	run;

	*Create dependent sort - controlling for PVOLdelta;
	proc sort  data=WorkDB_2; by Year Month P50VOLD_control; run;
	proc rank data= WorkDB_2 out= WorkDB_2 groups = 10;
		var C50VOLdelta;
		by Year Month P50VOLD_control;
		ranks C50VOLD_rk;
	run;

	*Create dependent sort - controlling for CVOLdelta;
	proc sort  data=WorkDB_2; by Year Month C50VOLD_control; run;
	proc rank data= WorkDB_2 out= WorkDB_2 groups = 10;
		var P50VOLdelta;
		by Year Month C50VOLD_control;
		ranks P50VOLD_rk;
	run;
%mend Get_WorkDB2;

%Get_WorkDB2 (db=WorkDB);

		/* Checks that ranking has been properly executed
			proc sql;
				create table test as 
				select Year, Month, P50VOLD_control,C50VOLD_rk, count(*) as ct
				from WorkDB_2
				group by Year, Month, P50VOLD_control,C50VOLD_rk
			;quit;
		*/


/*4.3: Macro to create results as per Table 3 of the paper					*/
%Macro Cal_Ret_TS_EW_DoubleSort (DB = , Rank = , Control = , FactorTS = , High = , Low = , MCap = ,
								 Lg0 = , Lg1 = , Lg2 = , Lg3 = , Lg4 = , Lg5 = , Lg6 = ,
								 Lg7 = , Lg8 = , Lg9 = , LgMean = );
/*
INPUT (definition of variables):
DB 			= Name of dataset 
Rank 		= Name of rank variable (Used for 2nd stage sorting)
Control		= Name of control variable (Used for 1st stage sorting)
FactorTS 	= Name of dataset containing factor returns
High 		= Rank 'value' of highest portfolio
Low 		= Rank 'value' of lower portfolio
Mcap		= Changes the portfolios from Equaly-Weighted to Mcap-weighted
Lg 			= Lag for computing Newey-West corrected tStat (0-9 for each 
			  control value, 'Mean' for avg. portfolio)

OUTPUT: 	Table3_&Control

PURPOSE:
- Replicate Table 3 from the paper. Generates a 10x10 cross section of average returns.
- This macro calculates Avg. 1 Month holding return (Price + Distributions) 
  on a portfolio which is rebalanced Monthly depending on ranks.
- Since the Portfolios are ranked over one variable controlling for the second, we 
  get a 10x10 matrix above.
- Portfolios are Equally Weighted and NOT Mcap weighted.
- The macro regresses the avg. return of Long Short Portfolio with Factors
- In the present macro we have included three models: (1) CAPM (2) FF3, and (3) FF5
- The model also calculates tStat using Newey-West correction, since error terms are
  auto-correlated.
*/

/* For debugging
%let db = WorkDB_2;				%let rank = C50VOLD_rk;		%let control = P50VOLD_control;
%let FactorTS = FF_Factors;		%let High = 9;				%let Low = 0;	%let MCap = No;
%let Lg0 = 6;	%let Lg1 = 6;	%let Lg2 = 6;	%let Lg3 = 6;	%let Lg4 = 6;
%let Lg5 = 6;	%let Lg6 = 6;	%let Lg7 = 6;	%let Lg8 = 6;	%let Lg9 = 6;
%let LgMean = 6;	%let Iter = 3;

*/

*Calculate Equal Weights for monthly portfolios;
proc sql;
	create table Temp_Weights as
	select Nxt_Year, Nxt_Month, &Control, &Rank, 1/count(*) as StockWt, count(*) as StockCount, sum(MktCap) as SumMcap
	from &DB
	where ~missing(&Rank) or ~missing(&Control)
	group by Nxt_Year, Nxt_Month, &Control, &Rank
;quit;

*Merge weights with return dataset (INNER JOIN);
proc sql;
	create table Temp_macroDB as
	select t1.*, t2.StockWt, t2.SumMcap
	from &DB as t1
		inner join Temp_Weights as t2
		on  t1.&Rank		=	t2.&Rank
		and t1.&Control		= 	t2.&Control
		and t1.Nxt_Year		=	t2.Nxt_Year
		and t1.Nxt_Month	=	t2.Nxt_Month
	order by t1.Nxt_Year, t1.Nxt_Month, t1.&Control, t1.&Rank
;quit;

*Overwrite Stock weights to be Market Weighted;
%if %upcase(&Mcap) = YES
	%then %do;
		data Temp_MacroDB;	set Temp_MacroDB;
			StockWt = MktCap/SumMCap;
		run;
	%end;

*Calculate Returns Time Series (decile portfolios);
proc sql;
	create table Ret_TS as
	select Nxt_Year, Nxt_Month, &Control, &Rank, 
		   count(StockWt) as Stocks, sum(ret_fwd1*StockWt) as PortRet
	from Temp_macroDB
	group by Nxt_Year, Nxt_Month, &Control, &Rank
	order by &Control, &Rank, Nxt_Year, Nxt_Month
;quit;

*Calcualte Returns Time Series (Long Short portfolio);
proc sql;
	create table Temp_LSport as 
	select t1.Nxt_Year, t1.Nxt_Month, t1.&Control, &high&Low as &Rank, (t1.PortRet - t2.PortRet) as PortRet
	from Ret_TS as t1
	inner join Ret_TS as t2
		on 	t1.Nxt_Year	 =	t2.Nxt_Year
		and t1.Nxt_Month =	t2.Nxt_Month
		and t1.&Control	 =  t2.&Control
		and t1.&Rank = &High
		and t2.&Rank = &Low
	order by &Control, Nxt_Year, Nxt_Month
;quit;

*Append Long-Short portfolio TS to RET_TS;
data Ret_TS; set Ret_TS Temp_LSport; run;

*Calculate Avg. Return for 100 portfolios + 10 LS portfolios;
proc sql;
	create table Table3a_avg as 
	select &Control, &Rank, Mean(PortRet) as AvgRet
	from Ret_TS
	group by &Control, &Rank
	order by &Control, &Rank
;quit;

*Transpose to Wide format;
proc transpose data=Table3a_avg out=Table3a_avg  (drop=_NAME_) prefix=&Rank;
	by &Control;
	id &Rank;
	var AvgRet;		format AvgRet 7.4;
run;


*Mean of LongShort Portfolios - WITH Newey-West correction;
/*proc sort data=Ret_TS; by &Control &Rank; run;*/

%let flag_createdb = 1; 
%do Iter = 0 %to 9;

/*%let Var = Lg&Iter;*/
/*%put &&&Var;*/

	*Calculate average value;
	proc model data=Temp_LSport (where=(&Control = &Iter));
	title "Mean of Long Short portfolios (Control value = &Iter)";
		endo PortRet;
		instruments / intonly;
		parms b0;
		PortRet=b0;
		fit PortRet / gmm kernel=(bart,%eval(&&Lg&Iter+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=Temp_Mean1;
	run; 
	quit;



	data Temp_Mean1;	set Temp_Mean1;	
		*Add dummy value to identify regression ;
		&Control = &Iter; format &Control 7.0;
		Lag = &&Lg&Iter; format Lag 7.0;
		&Rank = &High&Low; format &Rank 8.;

		*Keep only the tValue, Control and Rank;			
		drop Parameter EstType probt df Estimate StdErr;
	run;

	*Stack results in a dataset;
	%if (&flag_createdb = 1 ) %then %do;
		data Table3_tStat;
			set Temp_Mean1;
		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;								
		proc append base= Table3_tStat data=Temp_Mean1; run;	
	%end;
%end;

*Merge Avg. Returns (110 portfolios) + tStat for Long Short portfolios;
proc sql;
	create table Table3a_temp as
	select t1.*, t2.tValue, t2.Lag
	from Table3a_avg as t1
	left join Table3_tStat as t2
		on t1.&Control = t2.&Control
;quit;

*Calculate return of Long Short portfolio avgd. across all control ranks;
proc sql;
	create table Temp_LSPort_avg as
	select Nxt_Year, Nxt_Month, mean(PortRet) as PortRetAvg
	from Temp_LSPort
	group by Nxt_Year, Nxt_Month
	order by Nxt_Year, Nxt_Month
;quit;

*Add Factor Return Time Series;
proc sql;
	create table Temp_LSPort_Factors_TS as
	select t1.*,
		   t2.Rf, t2.MKT_Rf, t2.SMB3, t2.HML, t2.SMB5, t2.RMW, t2.CMA, t2.MOM
	from Temp_LSPort_avg	as t1
		left join &FactorTS as t2
		on  t1.Nxt_Year		=	t2.Year
		and t1.Nxt_Month	=	t2.Month
	order by t1.Nxt_Year, t1.Nxt_Month
;quit;

*Mean of avgd. Long Short Portfolio - WITH Newey-West correction;
proc model data=Temp_LSPort_Factors_TS;
title 'Mean of Long Short portfolios (avgd. across control values)';
	endo PortRetAvg;
	instruments / intonly;
	parms b0;
	PortRetAvg=b0;
	fit PortRetAvg / gmm kernel=(bart,%eval(&LgMean+1),0) vardef=n dw=10 dwprob;
	ods output ParameterEstimates=Temp_Mean2;
run; 
quit;

data Temp_Mean2;	set Temp_Mean2;	*Keep Estimate & tValue;			
	drop Parameter EstType probt df StdErr;
	Model = 'Average';
	Lag  = &LgMean; format Lag 7.0;
run;


*Regression using PROC MODEL - implements Newey-West correction;
proc model data=Temp_LSPort_Factors_TS ;
title 'CAPM';
		parms alpha b1;
		PortRetAvg = alpha + b1*Mkt_Rf;
        fit PortRetAvg / gmm kernel=(bart,%eval(&LgMean+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=temp_model1;
run; quit;
proc model data=Temp_LSPort_Factors_TS ;
title 'FF3';
		parms alpha b1 b2 b3;
		PortRetAvg = alpha + b1*Mkt_Rf + b2*SMB3 + b3*HML;
        fit PortRetAvg / gmm kernel=(bart,%eval(&LgMean+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=temp_model2;
run; quit;
proc model data=Temp_LSPort_Factors_TS ;
title 'FF5';
		parms alpha b1 b2 b3 b4 b5;
		PortRetAvg = alpha + b1*Mkt_Rf + b2*SMB5 + b3*HML + b4*RMW + b5*CMA;
        fit PortRetAvg / gmm kernel=(bart,%eval(&LgMean+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=temp_model3;
run; quit;
proc model data=Temp_LSPort_Factors_TS ;
title 'FF5_MOM';
		parms alpha b1 b2 b3 b4 b5 b6;
		PortRetAvg = alpha + b1*Mkt_Rf + b2*SMB5 + b3*HML + b4*RMW + b5*CMA +b6*MOM;
        fit PortRetAvg / gmm kernel=(bart,%eval(&LgMean+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=temp_model4;
run; quit;

*Calculate 'alpha' from different models;
proc sql;
create table Table3b_temp as 
	select *, 'CAPM' as Model
	from temp_model1
		UNION CORRESPONDING ALL
		select *, 'FF3' as Model
		from temp_model2
			UNION CORRESPONDING ALL
			select *, 'FF5' as Model
			from temp_model3
				UNION CORRESPONDING ALL
				select *, 'FF5_MOM' as Model
				from temp_model4
;quit;

data Table3b_temp; set Table3b_temp; 
	where Parameter = 'alpha'; 
	format Estimate tValue 7.4;
	Lag = &LgMean;	format Lag 	7.0;
	drop Parameter StdErr EstType probt df;	
run;


*Merge to create the final TABLE;
data Table3_&Control;
	set Table3a_temp Temp_Mean2 Table3b_temp;
run;

*Delete intermediate datasets;
proc datasets noprint; 
	delete	Temp_: Ret_TS Table3a_avg Table3_tStat Table3a_temp	Table3b_temp; 
run;

%Mend Cal_Ret_TS_EW_DoubleSort;

*EQUAL_WEIGHTED;
%Cal_Ret_TS_EW_DoubleSort (db = WorkDB_2, rank = C50VOLD_rk, control = P50VOLD_control, FactorTS = FF_Factors, High = 9, Low = 0, MCap = No,
						   Lg0 = 0, Lg1 = 0, Lg2 = 0, Lg3 = 2, Lg4 = 9, Lg5 = 7, Lg6 = 0, Lg7 = 0, Lg8 = 6, Lg9 = 9, LgMean = 6);
%Cal_Ret_TS_EW_DoubleSort (db = WorkDB_2, rank = P50VOLD_rk, control = C50VOLD_control, FactorTS = FF_Factors, High = 9, Low = 0, MCap = No,
						   Lg0 = 0, Lg1 = 0, Lg2 = 0, Lg3 = 0, Lg4 = 0, Lg5 = 10, Lg6 = 0, Lg7 = 9, Lg8 = 6, Lg9 = 8, LgMean = 0);

*MCAP_WEIGHTED;
%Cal_Ret_TS_EW_DoubleSort (db = WorkDB_2, rank = C50VOLD_rk, control = P50VOLD_control, FactorTS = FF_Factors, High = 9, Low = 0, MCap = Yes,
						   Lg0 = 0, Lg1 = 0, Lg2 = 0, Lg3 = 2, Lg4 = 9, Lg5 = 7, Lg6 = 0, Lg7 = 0, Lg8 = 6, Lg9 = 8, LgMean = 5);
%Cal_Ret_TS_EW_DoubleSort (db = WorkDB_2, rank = P50VOLD_rk, control = C50VOLD_control, FactorTS = FF_Factors, High = 9, Low = 0, MCap = Yes,
						   Lg0 =10, Lg1 = 10, Lg2 = 9, Lg3 = 9, Lg4 = 10, Lg5 = 0, Lg6 = 0, Lg7 = 10, Lg8 = 0, Lg9 = 0, LgMean = 2);



*****************************************************************************
3.4: TABLE V - LONG-TERM PREDICTABILITY
Comment:
Average monthly Holding Period Return = 
((1 + return for month1)*(1 + return for month2) ... (1 + return for monthN) - 1)/N
****************************************************************************;
data _Null_;run;
%Macro Cal_HPR_EW_DoubleSort (db = , rank = , control = , FactorTS = , High = , Low = , 
							  Lag1 = , Lag2 = , Lag3 = , Lag4 = , Lag5 = , Lag6 = );
/*
INPUT (definition of variables):
db 			= Name of dataset 
control		= Name of control variable (Used for 1st stage sorting)
rank 		= Name of rank variable (Used for 2nd stage sorting)
FactorTS 	= Name of dataset containing factor returns
High 		= Rank 'value' of highest portfolio
Low 		= Rank 'value' of lower portfolio
Lag (1-6) 	= Lag for computing Newey-West corrected tStat (one for each HPR regression)

OUTPUT:	

PURPOSE:
- Replicate Table 5 from the paper
- Using the double sorted portfolios from earlier, the macro calculates the average 
  monthly holding period return, if we hold the portfolio for upto 6 months.
- The idea of creating portfolio with overlapping holding periods comes from 
  Jegadeesh and Titman (1993). In their paper, for the holding period, they 
  consider both buy-and-hold strategy and monthly-rebalanced strategy. In this macro
  we only perform the monthly-rebalanced strategy.
- Portfolios are Equally Weighted and NOT Mcap weighted.
- The macro regresses the portfolio returns on FF3 Factors, and calculates tStat using 
  Newey-West correction, since error terms are auto-correlated.
*/

/* For debugging
%let db = WorkDB_2;				%let rank = C50VOLD_rk;		%let control = P50VOLD_control;
%let FactorTS = FF_Factors;		%let High = 9;				%let Low = 0;				
%let Lag1 = 6;		%let Lag2 = 5;		%let Lag3 = 8;
%let Lag4 = 8; 		%let Lag5 = 8;		%let Lag6 = 8;
*/

*Calculate Equal Weights for monthly portfolios;
*If a stock ceases to trade, we would allocate its weight to remaining stocks;
proc sql;
	create table Weights as
	select Nxt_Year, Nxt_Month, &Control, &Rank, 
			count(CASE WHEN ret_fwd1 is not NULL THEN 1 END) as ct1,
			count(CASE WHEN ret_fwd2 is not NULL THEN 1 END) as ct2,
			count(CASE WHEN ret_fwd3 is not NULL THEN 1 END) as ct3,
			count(CASE WHEN ret_fwd4 is not NULL THEN 1 END) as ct4,
			count(CASE WHEN ret_fwd5 is not NULL THEN 1 END) as ct5,
			count(CASE WHEN ret_fwd6 is not NULL THEN 1 END) as ct6
	from &db
	where ~missing(&Rank) or ~missing(&Control)
	group by Nxt_Year, Nxt_Month, &Control, &Rank;
quit;

	/* *Example;
	proc sql;
		create table test as
		select *
		from &db
		where Nxt_Year = 1996 and Nxt_Month = 3 and &Control = 0 and &Rank =0;
	quit;
	*/

*Merge weights with dataset (INNER JOIN);
proc sql;
	create table Temp_macroDB as
	select t1.*, 1/t2.ct1 as wt1, 1/t2.ct2 as wt2, 1/t2.ct3 as wt3, 
				 1/t2.ct4 as wt4, 1/t2.ct5 as wt5, 1/t2.ct6 as wt6
	from &db as t1
		inner join Weights as t2
		on  t1.&Rank		=	t2.&Rank
		and t1.&Control		= 	t2.&Control
		and t1.Nxt_Year		=	t2.Nxt_Year
		and t1.Nxt_Month	=	t2.Nxt_Month
	order by t1.Nxt_Year, t1.Nxt_Month, t1.&Control, t1.&Rank;
quit;

*Calculate Returns Time Series (1 month HPR for future months);
proc sql;
	create table Ret_TS as
	select Nxt_Year, Nxt_Month, &Control, &Rank, 
		   sum(wt1*ret_fwd1) as PortRet_m1,	   sum(wt2*ret_fwd2) as PortRet_m2,
		   sum(wt3*ret_fwd3) as PortRet_m3,	   sum(wt4*ret_fwd4) as PortRet_m4,
		   sum(wt5*ret_fwd5) as PortRet_m5,	   sum(wt6*ret_fwd6) as PortRet_m6
	from Temp_macroDB
	group by &Control, &Rank, Nxt_Year, Nxt_Month
	order by &Control, &Rank, Nxt_Year, Nxt_Month;
quit;

*Create Long Short portfolio (one for each rank of Control variables);
proc sql;
	create table Ret_TS_LSport as 
	select t1.Nxt_Year, t1.Nxt_Month, t1.&Control, &high&Low as &Rank, 
	   (t1.PortRet_m1 - t2.PortRet_m1) as LS_m1,   (t1.PortRet_m2 - t2.PortRet_m2) as LS_m2,
	   (t1.PortRet_m3 - t2.PortRet_m3) as LS_m3,   (t1.PortRet_m4 - t2.PortRet_m4) as LS_m4,
	   (t1.PortRet_m5 - t2.PortRet_m5) as LS_m5,   (t1.PortRet_m6 - t2.PortRet_m6) as LS_m6

	from Ret_TS as t1
		inner join Ret_TS as t2
		on 	t1.Nxt_Year	 =	t2.Nxt_Year
		and t1.Nxt_Month =	t2.Nxt_Month
		and t1.&Control	 =  t2.&Control
		and t1.&Rank = &High
		and t2.&Rank = &Low
	order by &Control, Nxt_Year, Nxt_Month;
quit;

*Create Long Short porfolio averaged across all ranks of Control variable;
proc sql;
	create table Ret_TS_LSPort_avg as
	select Nxt_Year, Nxt_Month, 
			Mean(LS_m1) as LS_avg_m1,	Mean(LS_m2) as LS_avg_m2,
			Mean(LS_m3) as LS_avg_m3,	Mean(LS_m4) as LS_avg_m4,
			Mean(LS_m5) as LS_avg_m5,	Mean(LS_m6) as LS_avg_m6

	from Ret_TS_LSport
	group by Nxt_Year, Nxt_Month
	order by Nxt_Year, Nxt_Month;
quit;

*Calculate Average (Per Month) Holding Period Return;
data Ret_TS_LSPort_avg;		set Ret_TS_LSPort_avg;
	LS_avg_hpr1m = ( (1+LS_avg_m1)																  		-1 )/1;
	LS_avg_hpr2m = ( (1+LS_avg_m1)*(1+LS_avg_m2)													  	-1 )/2;
	LS_avg_hpr3m = ( (1+LS_avg_m1)*(1+LS_avg_m2)*(1+LS_avg_m3)											-1 )/3;
	LS_avg_hpr4m = ( (1+LS_avg_m1)*(1+LS_avg_m2)*(1+LS_avg_m3)*(1+LS_avg_m4)							-1 )/4;
	LS_avg_hpr5m = ( (1+LS_avg_m1)*(1+LS_avg_m2)*(1+LS_avg_m3)*(1+LS_avg_m4)*(1+LS_avg_m5)			   	-1 )/5;
	LS_avg_hpr6m = ( (1+LS_avg_m1)*(1+LS_avg_m2)*(1+LS_avg_m3)*(1+LS_avg_m4)*(1+LS_avg_m5)*(1+LS_avg_m6)-1 )/6;
run;

*Find mean of each HPR TS;
%let flag_createdb = 1; 
%do iter = 1 %to 6;

	*Create dataset with required time series;
	data Temp_TS;	
		set Ret_TS_LSPort_avg;
		Keep LS_avg_hpr&iter.m;
	run;

	*Calculate average value;
	proc model data=Temp_TS ;
	title "Average HPR (&Iter month)";
		endo LS_avg_hpr&iter.m;
		instruments / intonly;
		parms b0;
		LS_avg_hpr&iter.m=b0;
		fit LS_avg_hpr&iter.m / gmm kernel=(bart,%eval(&Lag&Iter+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=Temp_Mean;
	run; 
	quit;

	*Add dummy value to output to identify the regression number;
	data Temp_Mean;	set Temp_Mean; 
		HPR = &Iter; format HPR 7.0; 
		Type = 'Mean';	format Type $20.;
		Keep Estimate tValue HPR Type;
	run;

	*Stack results in a dataset;
	*Append monthly files to Master Dataset (viz. Data_VS);
	%if (&flag_createdb = 1 ) %then %do;
		data Table5_mean;
			set Temp_Mean;
		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;								
		proc append base= Table5_mean data=Temp_Mean; run;	
	%end;
%end;


*Regress on FF3 - to be able to run this regression we need time series of 
 avg. hpr for the 3 factors, for holding period ranging from 1m to 6m;

data FF_Factors_hpr;
	set FF_Factors;
	Keep Year Month Rf Mkt_Rf SMB3 HML;
	rename Mkt_Rf	=	RmEx;
	rename SMB3		=	SMB;
run;

proc sort data=FF_Factors_hpr; by descending YEAR descending MONTH; run;

data FF_Factors_hpr;	set FF_Factors_hpr;

	RmEx_avg_hpr1m	=	( (1 + RmEx)																				-1)/1;
	RmEx_avg_hpr2m	=	( (1 + RmEx)*(1 + lag1(RmEx))																-1)/2;
	RmEx_avg_hpr3m	=	( (1 + RmEx)*(1 + lag1(RmEx))*(1+lag2(RmEx))												-1)/3;
	RmEx_avg_hpr4m	=	( (1 + RmEx)*(1 + lag1(RmEx))*(1+lag2(RmEx))*(1+lag3(RmEx))									-1)/4;
	RmEx_avg_hpr5m	=	( (1 + RmEx)*(1 + lag1(RmEx))*(1+lag2(RmEx))*(1+lag3(RmEx))*(1+lag4(RmEx))					-1)/5;
	RmEx_avg_hpr6m	=	( (1 + RmEx)*(1 + lag1(RmEx))*(1+lag2(RmEx))*(1+lag3(RmEx))*(1+lag4(RmEx))*(1+lag5(RmEx)) 	-1)/6;

	SMB_avg_hpr1m	=	( (1 + SMB)																				-1)/1;
	SMB_avg_hpr2m	=	( (1 + SMB)*(1 + lag1(SMB))																-1)/2;
	SMB_avg_hpr3m	=	( (1 + SMB)*(1 + lag1(SMB))*(1+lag2(SMB))												-1)/3;
	SMB_avg_hpr4m	=	( (1 + SMB)*(1 + lag1(SMB))*(1+lag2(SMB))*(1+lag3(SMB))									-1)/4;
	SMB_avg_hpr5m	=	( (1 + SMB)*(1 + lag1(SMB))*(1+lag2(SMB))*(1+lag3(SMB))*(1+lag4(SMB))					-1)/5;
	SMB_avg_hpr6m	=	( (1 + SMB)*(1 + lag1(SMB))*(1+lag2(SMB))*(1+lag3(SMB))*(1+lag4(SMB))*(1+lag5(SMB)) 	-1)/6;

	HML_avg_hpr1m	=	( (1 + HML)																				-1)/1;
	HML_avg_hpr2m	=	( (1 + HML)*(1 + lag1(HML))																-1)/2;
	HML_avg_hpr3m	=	( (1 + HML)*(1 + lag1(HML))*(1+lag2(HML))												-1)/3;
	HML_avg_hpr4m	=	( (1 + HML)*(1 + lag1(HML))*(1+lag2(HML))*(1+lag3(HML))									-1)/4;
	HML_avg_hpr5m	=	( (1 + HML)*(1 + lag1(HML))*(1+lag2(HML))*(1+lag3(HML))*(1+lag4(HML))					-1)/5;
	HML_avg_hpr6m	=	( (1 + HML)*(1 + lag1(HML))*(1+lag2(HML))*(1+lag3(HML))*(1+lag4(HML))*(1+lag5(HML)) 	-1)/6;
run;
proc sort data=FF_Factors_hpr; by YEAR MONTH; run;

*Merge with Portfolio holding period returns;
proc sql;
create table Ret_TS_HPR as
select t1.*, t2.*
from Ret_TS_LSPort_avg (drop= LS_avg_m1 LS_avg_m2 LS_avg_m3 LS_avg_m4 LS_avg_m5 LS_avg_m6) as t1
	left join FF_Factors_hpr as t2
	on 	t1.Nxt_Year 	= 	t2.Year
	and t1.Nxt_Month	= 	t2.Month
;quit;

*Run regressions for each HPR;
%let flag_createdb = 1; 
%do iter = 1 %to 6;
	*Create dataset with only regression variables;
	data Temp_Reg;	set Ret_TS_HPR;
		keep 	LS_Avg_hpr&iter.m 		RmEx_avg_hpr&iter.m 	
				SMB_avg_hpr&iter.m 		HML_avg_hpr&iter.m;
	run;

	proc model data=Temp_Reg ;
	title "FF3 alpha of HPR return (&Iter month)";
			parms alpha b1 b2 b3;
			LS_Avg_hpr&iter.m  = alpha + b1*RmEx_avg_hpr&iter.m + b2*SMB_avg_hpr&iter.m + b3*HML_avg_hpr&iter.m;
	        fit LS_Avg_hpr&iter.m  / gmm kernel=(bart,%eval(&&Lag&iter+1),0) vardef=n dw=10 dwprob;
			ods output ParameterEstimates=Temp_Reg_Output;
	run; 
	quit;

	*Add dummy value to output to identify the regression number;
	data Temp_Reg_Output;	set Temp_Reg_Output; 
		HPR = &Iter; format HPR 7.0; 
		Type = 'FF3_alpha';	format Type $20.;
	run;

	*Stack results in a dataset;
	%if (&flag_createdb = 1 ) %then %do;
		data Table5_reg;
			set Temp_Reg_Output;
		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;								
		proc append base= Table5_reg data=Temp_Reg_Output; run;	
	%end;
%end;

data Table5_reg; 
	set Table5_reg (where= (Parameter = 'alpha'));
	format Estimate 7.4;
	Keep HPR Estimate tValue Type;
run;

*Merge results for Mean and FF3 Regressions;
proc sql;
create table Table5_&control as 
select t1.HPR, 	t1.Estimate as Mean,	 	t1.tValue as tStat_mean,
				t2.Estimate as FF3_Alpha, t2.tValue as tStat_alpha
from Table5_mean as t1
	left join Table5_reg as t2
	on t1.HPR = t2.HPR
;quit;

data Table5_&control; set	Table5_&control;
	Mean = Mean *100;
	format Mean 7.2;
	FF3_Alpha = FF3_Alpha*100;
run;

*Delete intermediate datasets;
proc datasets noprint; 
	delete	Weights 	Temp_MacroDB 		FF_Factors_HPR
			RET_TS 		RET_TS_LSPORT		RET_TS_LSPORT_AVG	Temp_Mean
			Temp_TS		Ret_TS_HPR			Temp_Reg			Temp_Reg_Output	
			Table5_mean	Table5_reg;
run;

%Mend Cal_HPR_EW_DoubleSort;

%Cal_HPR_EW_DoubleSort (db = WorkDB_2, rank = C50VOLD_rk, control = P50VOLD_control, FactorTS = FF_Factors, High = 9, Low = 0, 
						Lag1 = 6, Lag2 = 5, Lag3 = 8, Lag4 = 8, Lag5 = 8, Lag6 = 8);
%Cal_HPR_EW_DoubleSort (db = WorkDB_2, rank = P50VOLD_rk, control = C50VOLD_control, FactorTS = FF_Factors, High = 9, Low = 0, 
						Lag1 = 0, Lag2 = 0, Lag3 = 1, Lag4 = 1, Lag5 = 2, Lag6 = 2);
****************************************************************************;



*****************************************************************************
3.5: TABLE IX - PREDICTING CROSS-SECTION OF IMPLIED & REALIZED VOLATILITIES
FAMA MACBETH REGRESSION - CROSS-SECTIONAL STAGE ONLY
****************************************************************************;
data _Null_;run;
%Macro Forecasting_regression(Inputdb= , DependentVar= ,IndependentVar=, VarCount=, LagArray=);

/* For Debugging
%let inputdb = WorkDB;			%let DependentVar = C50VOLdelta;
%let IndependentVar = ALPHA BETA SIZE BMlog MOM ILLIQ RVOL C_OI_delta P_OI_delta QSKEW;
%let VarCount = 11;				%let LagArray = 6 7 0 10 1 10 1 10 10 10 10;
*/

*Take Dependent Variable in another dataset, and create year/month variable to 
 match independent variables from previous month;
data Temp_MacroDB;
	set &inputdb;
	if Month > 1 then do; 	Match_Month = Month - 1;
							Match_Year	= Year; 	end;
				 else do;	Match_Month = 12;
							Match_Year	= Year - 1; end;
	keep Permno Year Month &DependentVar Match_Month Match_Year;
run;

*Merge Independent variables to Dependent variable 
 (INNER JOIN - data for Jan 1996 will get dropped);
proc sql;
	create table DB_Regression as
	select t1.*, t2.*
	from Temp_MacroDB as t1
		inner join &inputDB (Keep= PERMNO YEAR MONTH &IndependentVar) as t2
		on  t1.PERMNO 		= t2.PERMNO
		and t1.Match_Year 	= t2.Year
		and t1.Match_Month 	= t2.Month
	order by YEAR, MONTH
;quit;

*Change units for Open Interest variables to millions;
data DB_Regression; set DB_Regression;
	C_OI_delta 		= C_OI_delta/1000000;	*Convert to million;	
	P_OI_delta		= P_OI_delta/1000000;	*Convert to million;
	ILLIQ			= ILLIQ*100;			*ILLIQ uses Volume in $million originally. Converting it to $ would make coefficient 
											 very very small. Division by 100 is to make the output comparable with the results 
											 in the paper;
	where ~missing(C_OI_delta);				*While Proc REG automatically takes care of this, 
											 we do it to get rid of Feb 1996 observations, all of which have missing
											 value for change in Open Interest. This helps avoid SAS from throwing 
											 warning for the regression for that month;
run;

*Run Cross-Sectional regression for each month;
proc reg data=DB_Regression 
		 outest=Reg_op (where= (_TYPE_ in ('PARMS')) drop= &DependentVar _MODEL_ _RMSE_ Intercept _IN_ _P_ _EDF_) 
		 tableout rsquare;
	model &DependentVar = &IndependentVar 	/noprint;	/*Intercept is included by default*/
	by YEAR MONTH;
run;

*Average regression coefficients (averaged over all months) with Newey-West tStat;
%let VarAll = &IndependentVar _RSQ_;	*_RSQ_ is created in the regression above;

%let flag_createdb = 1; 
%do iter = 1 %to &VarCount; *One for each Independent variable used;

	%let Var 	= %scan(&VarAll,&iter);
	%let Lag 	= %scan(&LagArray, &iter); 

	*Calculate average value;
	proc model data=Reg_op;
	title "Mean of Independent Variable (&Var)";
		endo &Var;
		instruments / intonly;
		parms b0;
		&Var=b0;
		fit &Var / gmm kernel=(bart,%eval(&Lag+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=Temp_Mean;
	run; 
	quit;


	data Temp_Mean;	set Temp_Mean;	
		*Add dummy value to identify regression;
		length Control $ 20;
		Control = "&Var"; 

		*Keep only the tValue & Control;			
		Keep Estimate tValue Control;
	run;

	*Stack results in a dataset (Table9_&DependentVar);
	%if (&flag_createdb = 1 ) %then %do;
		data Table9_&DependentVar;
			set Temp_Mean;
		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;								
		proc append base= Table9_&DependentVar data=Temp_Mean; run;	
	%end;

%end;


*Makes two changes:
(1) Changes units of Dependent variable to %age points
(2) The variables in the dataset is difference of dPVOL - dCVOL while the Table needs for dCVOL - dPVOL;
%if %upcase(&DependentVar)= PDCDDIFF %then %do;
		data Table9_&DependentVar; set Table9_&DependentVar;
			if Control ~= '_RSQ_' then do ;
				Estimate = - Estimate*100; 
				tValue   = - tValue;
			end;
		run;
	%end;
	%else %do;
		data Table9_&DependentVar; set Table9_&DependentVar;
			Estimate = Estimate*100; 
		run;
	%end;

*Delete intermediate datasets;
proc datasets noprint; delete Temp_MacroDB DB_Regression Reg_op Temp_Mean; run;

%Mend Forecasting_regression;

%Forecasting_regression(inputdb= WorkDB, DependentVar= C50VOLdelta,	IndependentVar= ALPHA BETA SIZE BMlog MOM ILLIQ RVOL C_OI_delta P_OI_delta QSKEW,	VarCount= 11,	LagArray= 6 7 0 10 1 10 1 10 10 10 10);
%Forecasting_regression(inputdb= WorkDB, DependentVar= P50VOLdelta,	IndependentVar= ALPHA BETA SIZE BMlog MOM ILLIQ RVOL C_OI_delta P_OI_delta QSKEW, 	VarCount= 11,	LagArray= 5 0 6 3 1 10 6 9 8 2 10); 		
%Forecasting_regression(inputdb= WorkDB, DependentVar= PdCdDiff,	IndependentVar= ALPHA BETA SIZE BMlog MOM ILLIQ RVOL C_OI_delta P_OI_delta QSKEW, 	VarCount= 11,	LagArray= 0 2 1 1 1 10 1 9 10 10 0);
%Forecasting_regression(inputdb= WorkDB, DependentVar= RVOLdelta,	IndependentVar= ALPHA BETA SIZE BMlog MOM ILLIQ RVOL C_OI_delta P_OI_delta QSKEW, 	VarCount= 11,	LagArray= 5 9 3 10 9 10 10 10 10 10 6);

*To retain the order of variables in the PROC SQL merge below;
data Table9_C50VOLdelta; set Table9_C50VOLdelta;
	row = _N_;
run;

proc sql;
create table Table9 as
select t1.Control, 	t1.Estimate as CVOLd, t1.tValue as CVOLd_t,
					t2.Estimate as PVOLd, t2.tValue as PVOLd_t,
					t3.Estimate as CdPd,  t3.tValue as CdPd_t,
					t4.Estimate as RVOLd, t4.tValue as RVOLd_t	
from Table9_C50VOLdelta as t1
	left join Table9_P50VOLdelta as t2	on t1.Control = t2.Control
	left join Table9_PdCdDiff    as t3	on t1.Control = t3.Control
	left join Table9_RVOLdelta   as t4	on t1.Control = t4.Control
order by t1.row
;quit;

*Delete intermediate datasets;
proc datasets noprint; delete Table9_C50VOLdelta Table9_P50VOLdelta Table9_PdCdDiff Table9_RVOLdelta; run;

****************************************************************************;


*****************************************************************************
3.6: FIGURE I - IMPLIED VOLATILITIES PRE & POST FORMATION MONTHS
****************************************************************************;
data _Null_;run;

%Get_WorkDB2 (db=WorkDB);

%Macro Plot_ImpVol(DB = );

/*For Debugging
%let DB = WorkDB_2;	
*/

*PART I - Controlling for PVOL;
proc sql;
	create table Temp_Control_PVOL as  
	select *
	from &DB
	where ~missing(C50VOLD_rk) and ~missing(P50VOLD_control) and C50VOLD_rk in (0, 9)
	order by Nxt_Year, Nxt_Month, C50VOLD_rk
;quit;

*Create time series of average values;
proc sql;
	create table Temp_Control_PVOL_TS as  
	select C50VOLD_rk, Nxt_Year, Nxt_Month, 
			mean(CV_m6)  as Tm6, 	mean(CV_m5) as Tm5,		mean(CV_m4) as Tm4,	
			mean(CV_m3)  as Tm3,	mean(CV_m2) as Tm2,		mean(CV_m1) as Tm1,
			mean(C50VOL) as T0,
			mean(CV_p1)  as Tp1, 	mean(CV_p2) as Tp2,		mean(CV_p3) as Tp3,	
			mean(CV_p4)  as Tp4,	mean(CV_p5) as Tp5,		mean(CV_p6) as Tp6
	from Temp_Control_PVOL
	group by C50VOLD_rk, Nxt_Year, Nxt_Month
	order by C50VOLD_rk, Nxt_Year, Nxt_Month
;quit;

/* No action in implied volatility of Puts - since they have already been controlled;
proc sql;
	create table Temp_Control_PVOL_TS as  
	select C50VOLD_rk, Nxt_Year, Nxt_Month, 
			mean(PV_m6)  as Tm6, 	mean(PV_m5) as Tm5,		mean(PV_m4) as Tm4,	
			mean(PV_m3)  as Tm3,	mean(PV_m2) as Tm2,		mean(PV_m1) as Tm1,
			mean(P50VOL) as T0,
			mean(PV_p1)  as Tp1, 	mean(PV_p2) as Tp2,		mean(PV_p3) as Tp3,	
			mean(PV_p4)  as Tp4,	mean(PV_p5) as Tp5,		mean(PV_p6) as Tp6
	from Temp_Control_PVOL
	group by C50VOLD_rk, Nxt_Year, Nxt_Month
	order by C50VOLD_rk, Nxt_Year, Nxt_Month
;quit;
*/

*Average value over time series - for High and Low Rank;
proc sql;
	create table Figure_Call as
	select C50VOLD_rk as Rank,		
			mean(Tm6)  	as Tm6, 	mean(Tm5) as Tm5,		mean(Tm4) as Tm4,	
			mean(Tm3)  	as Tm3,		mean(Tm2) as Tm2,		mean(Tm1) as Tm1,
			mean(T0) 	as T0,
			mean(Tp1)  	as Tp1, 	mean(Tp2) as Tp2,		mean(Tp3) as Tp3,	
			mean(Tp4)  	as Tp4,		mean(Tp5) as Tp5,		mean(Tp6) as Tp6
	from Temp_Control_PVOL_TS
	group by C50VOLD_rk
;quit;

data Figure_Call; set Figure_Call; format _numeric_ 7.4 rank 7.0;	run;

proc transpose data=Figure_Call  out=Figure_Call (rename=(_NAME_ = Day))  ;
	id Rank;
	var Tm6 Tm5 Tm4 Tm3 Tm2 Tm1 T0 Tp1 Tp2 Tp3 Tp4 Tp5 Tp6;
run;


*PART II - Controlling for CVOL;
proc sql;
	create table Temp_Control_CVOL as  
	select *
	from &DB
	where ~missing(P50VOLD_rk) and ~missing(C50VOLD_control) and P50VOLD_rk in (0, 9)
	order by PERMNO, Nxt_Year, Nxt_Month
;quit;

*Create time series of average values;
proc sql;
	create table Temp_Control_CVOL_TS as  
	select P50VOLD_rk, Nxt_Year, Nxt_Month, 
			mean(PV_m6)  as Tm6, 	mean(PV_m5) as Tm5,		mean(PV_m4) as Tm4,	
			mean(PV_m3)  as Tm3,	mean(PV_m2) as Tm2,		mean(PV_m1) as Tm1,
			mean(P50VOL) as T0,
			mean(PV_p1)  as Tp1, 	mean(PV_p2) as Tp2,		mean(PV_p3) as Tp3,	
			mean(PV_p4)  as Tp4,	mean(PV_p5) as Tp5,		mean(PV_p6) as Tp6
	from Temp_Control_CVOL
	group by P50VOLD_rk, Nxt_Year, Nxt_Month
	order by P50VOLD_rk, Nxt_Year, Nxt_Month
;quit;

*Average value over time series - for High and Low Rank;
proc sql;
	create table Figure_Put as
	select P50VOLD_rk as Rank,		
			mean(Tm6)  	as Tm6, 	mean(Tm5) as Tm5,		mean(Tm4) as Tm4,	
			mean(Tm3)  	as Tm3,		mean(Tm2) as Tm2,		mean(Tm1) as Tm1,
			mean(T0) 	as T0,
			mean(Tp1)  	as Tp1, 	mean(Tp2) as Tp2,		mean(Tp3) as Tp3,	
			mean(Tp4)  	as Tp4,		mean(Tp5) as Tp5,		mean(Tp6) as Tp6
	from Temp_Control_CVOL_TS
	group by P50VOLD_rk
;quit;

data Figure_Put; set Figure_Put; format _numeric_ 7.4 Rank 7.0;	run;

proc transpose data=Figure_Put  out=Figure_Put (rename=(_NAME_ = Day))  ;
	id Rank;
	var Tm6 Tm5 Tm4 Tm3 Tm2 Tm1 T0 Tp1 Tp2 Tp3 Tp4 Tp5 Tp6;
run;

*Delete intermediate datesets;
proc datasets noprint; delete Temp_: ; run;

%Mend Plot_ImpVol;

%Plot_ImpVol (DB=WorkDB_2);


*****************************************************************************
3.7: TABLE X - PREDICTING IMPLIED AND REALIZED VOLATILITIES
****************************************************************************;
data _Null_;run;
/*Create dataset with information signal									*/
data WorkDb_3; 
	set WorkDB;
	where ~missing(Alpha);
run;


/*INDEPENDENT sorting of stocks each month									*/
*Rank;
proc sort  data=WorkDb_3; by Year Month; run;
proc rank data= WorkDb_3 out=WorkDb_3 groups = 10;
	var Alpha;
	by Year Month;
	ranks Alpha_rk;
run;


%Macro Table_X (DB= , LagC=, LagCP=, LagR=, LagP=);

/* For debugging
%let DB = WorkDB_3;		%let DepVar = C50VOLdelta;	%let ColName = CVOLd;
%let LagC = 6;	%let LagCP = 6;	%let LagR = 6;	%let LagP = 6;
*/


*Align next period value of DepVar;
proc sql;
	create table Temp_MacroDB as 
	select t1.*, t2.C50VOLdelta as C50VOLdelta_p1,
				 t2.P50VOLdelta as P50VOLdelta_p1
				 /*RVOLdelta_p1 already exists*/
	from &DB as t1
	left join &DB as t2
		on t1.SecurityID = t2.SecurityID
		and t1.DummyDt	 = intnx('month',t2.DummyDt,-1)
	order by SecurityID, Dummydt
;quit;


*Create time series of average values;
proc sql;
	create table Temp_Mean_TS as 
	select Alpha_rk, Year, Month,
					 mean(C50VOLdelta_p1) as  C,
					 mean(C50VOLdelta_p1-P50VOLdelta_p1) as C_P,
					 mean(RVOLdelta_p1) as R,
					 mean(P50VOLdelta_p1) as P
	from Temp_MacroDB
	group by Alpha_rk, Year, Month
	order by Alpha_rk, Year, Month
;quit;

*Calculate avg. value from time series;
proc sql;
	create table Temp_Mean as 
	select Alpha_rk, mean(C) as  Mean_C,
					 mean(C_P) as Mean_CP,
					 mean(R) as Mean_R,
					 mean(P) as Mean_P
	from Temp_Mean_TS
	group by Alpha_rk
	order by Alpha_rk
;quit;

*Format and save in Table10;
Data Table10; set Temp_Mean;
	format Mean_C Mean_CP Mean_R Mean_P 7.4;
	newvar = put(alpha_rk, 8.);
	drop alpha_rk;	rename  newvar = alpha_rk;
run;

*Mothly Time series of LongShort Portfolio;
proc sql;
	create table Temp_LS as 
	select t1.year, t1.month, 90 as Alpha_rk,
			(t1.C - t2.C) as C, (t1.C_P - t2.C_P) as C_P,
			(t1.R - t2.R) as R, (t1.P - t2.P) as P
	from Temp_Mean_TS as t1
	inner join Temp_Mean_TS as t2
		on t1.Year = t2.Year
		and t1.Month = t2.Month
		and t1.Alpha_rk = 9
		and t2.Alpha_rk = 0
;quit;

*Mean of LongShort Portfolio - WITH Newey-West correction;
proc model data=Temp_LS ;	title 'Mean of CVOLd';
	endo C;		instruments / intonly;
	parms mean_C;	C=mean_C;
	fit C / gmm kernel=(bart,%eval(&LagC+1),0) vardef=n dw=10 dwprob;
	ods output ParameterEstimates=Temp_Mean_C;
run; quit;

proc model data=Temp_LS ;	title 'Mean of CVOLd-PVOLd';
	endo C_P;		instruments / intonly;
	parms mean_CP;	C_P=mean_CP;
	fit C_P / gmm kernel=(bart,%eval(&LagCP+1),0) vardef=n dw=10 dwprob;
	ods output ParameterEstimates=Temp_Mean_CP;
run; quit;

proc model data=Temp_LS ;	title 'Mean of RVOLd';
	endo R;		instruments / intonly;
	parms mean_R;	R=mean_R;
	fit R / gmm kernel=(bart,%eval(&LagR+1),0) vardef=n dw=10 dwprob;
	ods output ParameterEstimates=Temp_Mean_R;
run; quit;

proc model data=Temp_LS ;	title 'Mean of PVOLd';
	endo P;		instruments / intonly;
	parms mean_P;	P=mean_P;
	fit P / gmm kernel=(bart,%eval(&LagP+1),0) vardef=n dw=10 dwprob;
	ods output ParameterEstimates=Temp_Mean_P;
run; quit;

*Merge the Mean values from all the four models;
data Temp_mean;
	set Temp_Mean_C Temp_Mean_CP Temp_Mean_R Temp_Mean_P;
	Keep Parameter Estimate tValue;
	format Estimate 7.4 tValue 7.2;
run;

*Transpose;
proc transpose data=Temp_mean  out=Temp_mean (drop=_LABEL_ rename=(_NAME_=Alpha_Rk))  ;
	id Parameter;
	var Estimate tValue;
run;

*Append values for LongShort portfolio to Table10;
data Table10;
	retain Alpha_rk;
	set Table10 temp_mean;
run;

*Delete temporary dataset;
proc datasets noprint; delete Temp_: ; run;

%Mend Table_X;

%Table_X (DB= WorkDB_3, LagC= 6, LagCP= 1, LagR= 10, LagP=4);


*****************************************************************************
3.8: TABLE A - DOES PREDICTABILITY VARY BY STOCK CHARACTERISTIC?
****************************************************************************;
data _Null_;run;

/*Remove obs with missing next month return									*/
data WorkDB_4;
	set WorkDB;
	where ~missing(ret_fwd1);
run;


%Macro Test_Mcap (DB = , Rank = , Control = , Grp1=, Grp2=, MCap = ,	LagArray= );

/* For debugging
%let db = WorkDB_4;			%let rank = C50VOLD_rk;		%let control = SIZE;
%let Grp1 = 5;				%let Grp2 =5;				
%let LagArray = 0 6 3 5 0;
*/

*Create control rankings (independent);
proc sort  data=&DB; by Year Month &Control; run;
proc rank data= &DB out= Temp_DB groups = &Grp1;
	var &Control;
	by Year Month;
	ranks &Control._rk;
run;


*Unlike previous macros, here we create both the ranked variables,
 however use only the one provided as an input parameter;
*Create dependent sort C50VOLdeta;
proc sort  data=Temp_DB; by Year Month &Control._rk; run;
proc rank data= Temp_DB out= Temp_DB groups = &Grp2;
	var C50VOLdelta;
	by Year Month &Control._rk;
	ranks C50VOLD_rk;
run;

*Create dependent sort on P50VOLdeta;
proc sort  data=Temp_DB; by Year Month &Control._rk; run;
proc rank data= Temp_DB out= Temp_DB groups = &Grp2;
	var P50VOLdelta;
	by Year Month &Control._rk;
	ranks P50VOLD_rk;
run;


*Calculate Returns Time Series (decile portfolios);
proc sql;
	create table Ret_TS as
	select Nxt_Year, Nxt_Month, &Control._rk, &Rank, 
		   count(*) as Stocks, mean(ret_fwd1)*100 as PortRet, 
		   mean(C50VOLdelta) as CIVavg, mean(P50VOLdelta) as PIVavg,
		   mean(C50VOLdelta/(C50vol-C50VOLdelta)) as CVOLchangepct,
		   mean(P50VOLdelta/(P50vol-P50VOLdelta)) as PVOLchangepct
	from Temp_DB
	where ~missing(&Rank) and ~missing(&Control._rk)
	group by &Control._rk, &Rank, Nxt_Year, Nxt_Month
	order by &Control._rk, &Rank, Nxt_Year, Nxt_Month
;quit;

*Calcualte Returns Time Series (Long Short portfolio);
proc sql;
	create table Temp_LSport as 
	select t1.Nxt_Year, t1.Nxt_Month, t1.&Control._rk, %eval(&Grp2-1)0 as &Rank,
		  (t1.PortRet - t2.PortRet) as PortRet, 
		  (t1.CIVavg - t2.CIVavg) as CIVavg,	/*Spread in IV between extreme portfolios*/
		  (t1.PIVavg - t2.PIVavg) as PIVavg

	from Ret_TS as t1
	inner join Ret_TS as t2
		on 	t1.Nxt_Year	 =	t2.Nxt_Year
		and t1.Nxt_Month =	t2.Nxt_Month
		and t1.&Control._rk	 =  t2.&Control._rk
		and t1.&Rank = %eval(&Grp2-1)
		and t2.&Rank = 0
	order by &Control._rk, Nxt_Year, Nxt_Month
;quit;

*Append Long-Short portfolio TS to RET_TS;
data Ret_TS; set Ret_TS Temp_LSport; run;

*Calculate Avg. Return for bivariate portfolios + LS portfolios;
proc sql;
	create table Table_avg as 
	select &Control._rk, &Rank, Mean(PortRet) as AvgRet, 
			Mean(CIVavg) as CIVavg,
			Mean(PIVavg) as PIVavg
	from Ret_TS
	group by &Control._rk, &Rank
	order by &Control._rk, &Rank
;quit;

*Take Implied Volatility spread  to another dataset;
data Data_IVspread; set Table_avg;
	where &Rank = %eval(&Grp2-1)0;
run;

*Calculate average change in volume for different levels of control variable;
proc sql;
	create table Temp_VolChange as 
	select Nxt_Year, Nxt_Month,&Control._rk, 
			mean(CVOLchangepct) as CVOLchangepct, 
			mean(PVOLchangepct) as PVOLchangepct
	from Ret_TS
	group by &Control._rk, Nxt_Year, Nxt_Month
;quit;
	
proc sql;
	create table Data_VolChange as 
	select &Control._rk, 
			mean(CVOLchangepct) as CVOLchangepct, 
			mean(PVOLchangepct) as PVOLchangepct
	from Temp_VolChange
	group by &Control._rk
;quit;

*Transpose to Wide format;
proc transpose data=Table_avg out=Table_avg  (drop=_NAME_) prefix=&Rank;
	by &Control._rk;
	id &Rank;
	var AvgRet;		format AvgRet 7.4;
run;

*Mean of LongShort Portfolios - WITH Newey-West correction;
%let flag_createdb = 1; 
%do Iter = 0 %to %eval(&Grp1-1);

	%let Lag 	= %scan(&LagArray, %eval(&iter+1));
 
	*Calculate average value;
	proc model data=Temp_LSport (where=(&Control._rk = &Iter));
	title "Mean of Long Short portfolios (Control value = &Iter)";
		endo PortRet;
		instruments / intonly;
		parms b0;
		PortRet=b0;
		fit PortRet / gmm kernel=(bart,%eval(&Lag+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=Temp_Mean1;
	run; 
	quit;


	data Temp_Mean1;	set Temp_Mean1;	
		*Add dummy value to identify regression ;
		&Control._rk = &Iter; 				format &Control._rk 7.0;
		Lag 	 	 = &Lag; 				format Lag 7.0;
		&Rank 	 	 = %eval(&Grp2-1)0; 	format &Rank 8.;

		*Keep only the tValue, Control and Rank;			
		drop Parameter EstType probt df Estimate StdErr;
	run;

	*Stack results in a dataset;
	%if (&flag_createdb = 1 ) %then %do;
		data Table_tStat;
			set Temp_Mean1;
		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;								
		proc append base= Table_tStat data=Temp_Mean1; run;	
	%end;
%end;

*Merge Avg. Returns + tStat for Long Short portfolios;
proc sql;
	create table Table_A_&Rank as
	select t1.*, t2.tValue, t2.Lag
	from Table_avg as t1
	left join Table_tStat as t2
		on t1.&Control._rk = t2.&Control._rk
;quit;


*Delete intermediate datasets;
proc datasets noprint; 
	delete	Temp_: Ret_TS Table_avg Table_tStat; 
run;

%Mend Test_Mcap;

*EQUAL_WEIGHTED;
%Test_Mcap (DB = WorkDB_4, Rank = C50VOLD_rk, Control = SIZE, Grp1= 5, Grp2= 5, LagArray= 0 6 3 5 0);
%Test_Mcap (DB = WorkDB_4, Rank = P50VOLD_rk, Control = SIZE, Grp1= 5, Grp2= 5, LagArray= 0 3 3 3 3);

%Test_Mcap (DB = WorkDB_4, Rank = C50VOLD_rk, Control = ILLIQ, Grp1= 5, Grp2= 5, LagArray= 5 5 2 4 6);
%Test_Mcap (DB = WorkDB_4, Rank = P50VOLD_rk, Control = ILLIQ, Grp1= 5, Grp2= 5, LagArray= 5 3 3 5 10);

%Test_Mcap (DB = WorkDB_4, Rank = C50VOLD_rk, Control = C_VOLUME, Grp1= 5, Grp2= 5, LagArray= 0 6 9 2 3);
%Test_Mcap (DB = WorkDB_4, Rank = P50VOLD_rk, Control = P_VOLUME, Grp1= 5, Grp2= 5, LagArray= 0 3 3 3 0);

%Test_Mcap (DB = WorkDB_4, Rank = C50VOLD_rk, Control = C_VOLUME_DELTA, Grp1= 5, Grp2= 5, LagArray= 9 7 8 6 9);
%Test_Mcap (DB = WorkDB_4, Rank = P50VOLD_rk, Control = P_VOLUME_DELTA, Grp1= 5, Grp2= 5, LagArray= 3 3 3 3 3);

proc sql;
	create table test as
	select nxt_year, nxt_month, mean(C50VOLdelta/(C50vol-C50VOLdelta)) as c, mean(P50VOLdelta/(P50vol-P50VOLdelta)) as p
	from WorkDB_4
	group by nxt_year, nxt_month
;quit;



*****************************************************************************
3.9: TABLE IV - DESCRIPTIVE STATISTICS FOR BIVARIATE SORTED PORTFOLIOS
****************************************************************************;
data _Null_;run;

*Get Bivariate Sorted DB called WorkDB_2 (created from WorkDB);
%Get_WorkDB2(db=WorkdB);

%Macro Table4(DB=, Rank=, Control=, FactorTS= );

/* For debugging
%let DB = WorkDB_2;			%let rank = C50VOLD_rk;		%let control = P50VOLD_control;
%let FactorTS = FF_Factors;	%let Iter = 3;
*/

*Create a subset DB;
Data Temp_MacroDB; 
	set &DB;
	where ~missing(&Rank) and ~missing(&Control);
run;


proc sort data=Temp_MacroDB; by &Rank Year Month ; run;

/*
*Incase you want Mean values instead of Median values. Results remain unchanged;
proc means data = Temp_MacroDB noprint;
	var BETA SIZE BM MOM  REV ILLIQ SKEW COSKEW QSKEW Ret_Fwd1;
	by &Rank Year Month;
	output out=Temp_median (drop= _TYPE_ _FREQ_)	
			mean(BETA)=	mean(SIZE)=	mean(BM)=
			mean(MOM)=	mean(REV)=	mean(ILLIQ)=
			mean(SKEW)=	mean(COSKEW)=	mean(QSKEW)= 
			mean(Ret_fwd1)= /AUTONAME;
run; 
proc sql;
	create table Temp_Table as 
	select &Rank, 
			mean(RET_FWD1_MEAN) as Ret, 	mean(BETA_mean) as BETA, 		
			mean(SIZE_mean) as SIZE,		mean(BM_mean)	as BM,
			mean(MOM_mean) as MOM,		mean(REV_mean) as REV,
			mean(ILLIQ_mean) as ILLIQ, 	mean(SKEW_Mean) as SKEW,
			mean(COSKEW_mean) as COSKEW,	mean(QSKEW_mean) as QSKEW
	from Temp_Median
	group by &Rank
;quit;
*/

proc means data = Temp_MacroDB noprint;
	var BETA SIZE BM MOM  REV ILLIQ SKEW COSKEW QSKEW Ret_Fwd1;
	by &Rank Year Month;
	output out=Temp_median (drop= _TYPE_ _FREQ_)	
			median(BETA)=	median(SIZE)=	median(BM)=
			median(MOM)=	median(REV)=	median(ILLIQ)=
			median(SKEW)=	median(COSKEW)=	median(QSKEW)= 
			mean(Ret_fwd1)= /AUTONAME;
run; 
proc sql;
	create table Temp_Table as 
	select &Rank, 
			mean(RET_FWD1_MEAN) as Ret, 	mean(BETA_median) as BETA, 		
			mean(SIZE_median) as SIZE,		mean(BM_median)	as BM,
			mean(MOM_median) as MOM,		mean(REV_median) as REV,
			mean(ILLIQ_median) as ILLIQ, 	mean(SKEW_Median) as SKEW,
			mean(COSKEW_median) as COSKEW,	mean(QSKEW_median) as QSKEW
	from Temp_Median
	group by &Rank
;quit;

*Calculate alpha for Return time series;
data Temp_Ret_TS;
	set Temp_median;
	keep &Rank Year Month Ret_fwd1_mean;
run;

*Merge Factor values from the Factor dataset;
proc sql;
	create table Temp_RET_FF_TS as
	select t1.*, (t1.Ret_fwd1_mean-t2.Rf) as RetEx, t2.Rf, t2.MKT_Rf, t2.SMB3, t2.HML, t2.SMB5, t2.RMW, t2.CMA, t2.MOM
	from Temp_Ret_TS as t1
	left join &FactorTS as t2
		on mdy(t1.month, 1, t1.Year) = intnx('month',mdy(t2.month,1,t2.Year),-1)
	order by &Rank, Year, Month
;quit;	

*Run regression to find intercept for each rank;
proc reg data=Temp_RET_FF_TS 
		 outest=Temp_Reg_op (where= (_TYPE_ in ('PARMS')))
		 tableout;
	FF5_MOM : model RetEx = Mkt_Rf SMB5 HML RMW CMA	MOM /noprint;	
	by &Rank;
run;

*Format Regression output;
data Temp_Reg_op; set Temp_Reg_op; Keep &Rank Intercept; format Intercept 7.4; run;

*Merge intercept from regression to table with average values;
proc sql;
	create table Table4_&Rank as 
	select t1.&Rank, t1.Ret, t2.Intercept as FF5_MOM, t1.*
	from Temp_Table as t1
	left join Temp_Reg_op as t2
		on t1.&Rank = t2.&Rank
	order by &Rank
;quit;

data Table4_&Rank; set Table4_&Rank;
	format _numeric_ 7.4;
	format &Rank 7.0;

	*Changing units from decimal to percentages;
	ILLIQ = ILLIQ*100;		

	*ILLIQ in the authors DB seem to be 100x that in WorkDB.
	 Possibly because they take the Return in the numerator
	 in percentages instead of decimals;
	Ret = Ret*100;	FF5_MOM = FF5_MOM*100;	MOM = MOM*100;			
	REV = REV*100;	QSKEW = QSKEW*100;
run;

proc datasets noprint; delete Temp_: ; run;

%mend Table4;

%Table4 (DB=WorkDB_2, Rank = C50VOLD_rk, Control = P50VOLD_control, FactorTS = FF_Factors);
%Table4 (DB=WorkDB_2, Rank = P50VOLD_rk, Control = C50VOLD_control, FactorTS = FF_Factors);


*****************************************************************************
3.10: TABLE C - SIMILAR TO TABLE V, BUT WITH INCREMENTAL RETURN ONLY
****************************************************************************;
data _Null_;run;

*Get Bivariate Sorted DB called WorkDB_2 (created from WorkDB);
%Get_WorkDB2(db=WorkdB);

%Macro TableC (DB = , Rank = , Control = , FactorTS = , LagArray =);

/* For debugging
%let DB = WorkDB_2;				%let rank = C50VOLD_rk;		%let control = P50VOLD_control;
%let FactorTS = FF_Factors;		%let LagArray = 6 6 6 6 6 6;	%let Iter = 3;
*/

data Temp_MacroDB;
	set &DB;
	where ~missing(&Rank) and ~missing(&Control) and &Rank in (0,9);
run;

*Calculate Returns Time Series (1 month return for future months);
proc sql;
	create table Temp_Ret_TS as
	select Nxt_Year, Nxt_Month, &Rank, 
		   mean(ret_fwd1) as PortRet_m1,	   mean(ret_fwd2) as PortRet_m2,
		   mean(ret_fwd3) as PortRet_m3,	   mean(ret_fwd4) as PortRet_m4,
		   mean(ret_fwd5) as PortRet_m5,	   mean(ret_fwd6) as PortRet_m6
	from Temp_MacroDB
	group by &Rank, Nxt_Year, Nxt_Month
	order by &Rank, Nxt_Year, Nxt_Month;
quit;

*Create Long Short portfolio;
proc sql;
	create table Temp_LS_Ret_TS as 
	select t1.Nxt_Year, t1.Nxt_Month, &high&Low as &Rank, 
	   (t1.PortRet_m1 - t2.PortRet_m1) as LS_m1,   (t1.PortRet_m2 - t2.PortRet_m2) as LS_m2,
	   (t1.PortRet_m3 - t2.PortRet_m3) as LS_m3,   (t1.PortRet_m4 - t2.PortRet_m4) as LS_m4,
	   (t1.PortRet_m5 - t2.PortRet_m5) as LS_m5,   (t1.PortRet_m6 - t2.PortRet_m6) as LS_m6

	from Temp_Ret_TS as t1
		inner join Temp_Ret_TS as t2
		on 	t1.Nxt_Year	 =	t2.Nxt_Year
		and t1.Nxt_Month =	t2.Nxt_Month
		and t1.&Rank = 9
		and t2.&Rank = 0
	order by Nxt_Year, Nxt_Month;
quit;

/*
*Average over time series;
proc sql;
	create table Temp_LS_Ret_Avg as
	select 	Mean(LS_m1) as LS_avg_m1,	Mean(LS_m2) as LS_avg_m2,
			Mean(LS_m3) as LS_avg_m3,	Mean(LS_m4) as LS_avg_m4,
			Mean(LS_m5) as LS_avg_m5,	Mean(LS_m6) as LS_avg_m6
	from Temp_LS_Ret_TS
quit;
*/

%let flag_createdb = 1; 
%do iter = 1 %to 6;

	%let Lag 	= %scan(&LagArray, &iter);

	*Calculate average value;
	proc model data=Temp_LS_Ret_TS ;
	title "1 month return (for month number &Iter)";
		endo LS_m&iter;
		instruments / intonly;
		parms b0;
	 	LS_m&iter=b0;
		fit  LS_m&iter / gmm kernel=(bart,%eval(&Lag+1),0) vardef=n dw=10 dwprob;
		ods output ParameterEstimates=Temp_Mean;
	run; 
	quit;
	*Add dummy value to output to identify the regression number;
	data Temp_Mean;	set Temp_Mean; 
		Month = &Iter; format Month 7.0;
		Lag  = &Lag; format Lag 7.0; 
		Keep Estimate tValue Month Lag;
	run;

	*Stack results in a dataset;
	%if (&flag_createdb = 1 ) %then %do;
		data TableC_Mean;
			set Temp_Mean;
		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;								
		proc append base= TableC_Mean data=Temp_Mean; run;	
	%end;
%end;




*Regress on FF3 - to be able to run this regression we need time series of 
 avg. hpr for the 3 factors, for holding period ranging from 1m to 6m;

data FF_Factors_hpr;
	set FF_Factors;
	Keep Year Month Rf Mkt_Rf SMB3 HML;
	rename Mkt_Rf	=	RmEx;
	rename SMB3		=	SMB;
run;
proc sort data=FF_Factors_hpr; by descending YEAR descending MONTH; run;

data FF_Factors_hpr;	set FF_Factors_hpr;
	RmEx_1m	= 	RmEx		;	RmEx_2m	=	lag1(RmEx)	;	RmEx_3m	=	lag2(RmEx)	;
	RmEx_4m	=	lag3(RmEx)	;	RmEx_5m	=	lag4(RmEx)	;	RmEx_6m	=	lag5(RmEx)	;

	SMB_1m	=	SMB			;	SMB_2m	=	lag1(SMB)	;	SMB_3m	=	lag2(SMB)	;
	SMB_4m	=	lag3(SMB)	;	SMB_5m	=	lag4(SMB)	;	SMB_6m	=	lag5(SMB)	;

	HML_1m	=	HML			;	HML_2m	= 	lag1(HML)	;	HML_3m	=	lag2(HML)	;
	HML_4m	=	lag3(HML)	;	HML_5m	=	lag4(HML)	;	HML_6m	=	lag5(HML)	;
run;
proc sort data=FF_Factors_hpr; by YEAR MONTH; run;

*Merge with Portfolio incremental returns;
proc sql;
	create table Ret_TS_HPR as
	select t1.*, t2.*
	from Temp_LS_Ret_TS  as t1
		left join FF_Factors_hpr as t2
		on 	t1.Nxt_Year 	= 	t2.Year
		and t1.Nxt_Month	= 	t2.Month
;quit;

*Run regressions for each incremental return;
%let flag_createdb = 1; 
%do iter = 1 %to 6;

	%let Lag 	= %scan(&LagArray, &iter);

	*Create dataset with only regression variables;
	data Temp_Reg;	set Ret_TS_HPR;
		keep 	LS_m&iter	 		RmEx_&iter.m 	
				SMB_&iter.m 		HML_&iter.m;
	run;

	proc model data=Temp_Reg ;
	title "FF3 alpha of incremental return (&Iter month)";
			parms alpha b1 b2 b3;
			LS_m&iter  = alpha + b1*RmEx_&iter.m + b2*SMB_&iter.m + b3*HML_&iter.m;
	        fit LS_m&iter  / gmm kernel=(bart,%eval(&Lag+1),0) vardef=n dw=10 dwprob;
			ods output ParameterEstimates=Temp_Reg_Output;
	run; 
	quit;

	*Add dummy value to output to identify the regression number;
	data Temp_Reg_Output;	set Temp_Reg_Output; 
		Month = &Iter; format Month 7.0; 
		Type = 'FF3_alpha';	format Type $20.;
	run;

	*Stack results in a dataset;
	%if (&flag_createdb = 1 ) %then %do;
		data TableC_reg;
			set Temp_Reg_Output;
		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;								
		proc append base= TableC_reg data=Temp_Reg_Output; run;	
	%end;
%end;

data TableC_reg; 
	set TableC_reg (where= (Parameter = 'alpha'));
	format Estimate 7.4;
	Keep Month Estimate tValue Type;
run;

*Merge results for Mean and FF3 Regressions;
proc sql;
	create table TableC_&Rank as 
	select t1.Month, 	t1.Estimate as Mean,	 	t1.tValue as tStat_mean,
		   t2.Estimate as FF3_Alpha, 				t2.tValue as tStat_alpha,
		   t1.Lag as Lag
	from TableC_Mean as t1
		left join TableC_reg as t2
		on t1.Month = t2.Month
;quit;


*Change estimate from decimal to percentage;
Data TableC_&Rank;	set TableC_&Rank;
	Mean = Mean*100;
	FF3_Alpha = FF3_Alpha *100;
run;

*Delete intermediate datasets;
proc datasets noprint; delete Temp_: TableC_reg TableC_mean FF_Factors_hpr RET_TS_HPR; run;

%Mend TableC;
%TableC (db = WorkDB_2, rank = C50VOLD_rk, control = P50VOLD_control, FactorTS = FF_Factors, LagArray = 6 0 8 10 0 0);
%TableC (db = WorkDB_2, rank = P50VOLD_rk, control = C50VOLD_control, FactorTS = FF_Factors, LagArray = 0 0 2 0 5 10);
****************************************************************************;


