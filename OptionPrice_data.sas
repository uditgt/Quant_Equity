******************************************************************************
PURPOSE		: 	The following code is to work with data from underlying 
				option contracts and their prices
INPUT		: 	Preliminaries in Step 1 and Step 3
OUTPUT		: 	Produces a dataset with start and end values for option 
				contract, along with a combination of return variables.
COMMENTS	:	This code has a lot in common with Paper_replication code, in 
				terms of how macro variable is created (Step 1), and pulled
				data is matched with PERMNO (Step 3)
AUTHOR		:	UDIT GUPTA (September, 2015)
*****************************************************************************

*****************************************************************************
CODE OVERVIEW:
STEP 1: CREATE MACRO VARIABLE WITH FILE NAMES FOR ALL MONTHLY FILES
STEP 2: PULL DATA FROM OPTION_PRICE DATAFILES.
STEP 3: MERGE OPTION PRICE DATA WITH CRSP PRICES
STEP 4: ADD ALPHA SIGNAL
STEP 5: RUN BACKTEST
STEP 6: EXAMPLE FROM GOYAL & SARETTO (2009)
****************************************************************************;

*****************************************************************************
PRELIMINARIES
****************************************************************************;
* Setting up ODBC connection, and loading the Schema;
libname IVyDB odbc complete=' DSN=SQLServerProdN;Database=IVyDB ' schema=dbo;

* Location of Dataset folder;
libname DBfolder ' \\dev-sasbi101\temp\Gupta\Datasets ';

* USER INPUTS;
* Time Series to be pulled;
%let ts_start_yr = 1996;
%let ts_start_mt = 1;
%let ts_end_yr   = 2013;		*last date file is 2014_08;
%let ts_end_mt   = 12;

* Provide Database dates;
%let dirname		= IvyDB. 				;
%let db_name1		= Option_Price_ 		;	*used in pull_OP_data1 & pull_OP_data2;
%let db_start_yr	= 1996 ;	
%let db_start_mt	= 1    ;
%let db_end_yr		= 2015 ;	
%let db_end_mt		= 7    ;

*****************************************************************************
STEP 1: CREATE MACRO VARIABLE WITH FILE NAMES FOR ALL MONTHLY FILES
1.1. Create file names as per the User Input in the macro variable above
1.2. Create a Macro Variable containing all these names
1.3. Count the number of names (used when looping)
****************************************************************************;

/*1.1. Create file names as per the User Input in the macro variable above	*/
%Macro get_filenames( ts_start_yr, ts_start_mt, ts_end_yr, ts_end_mt);

data List_filenames;

date = mdy(&ts_start_mt,1,&ts_start_yr);
 
do while (date <= mdy(&ts_end_mt,1,&ts_end_yr));
    output;
    date = intnx('month', date, 1, 's');
end;

run;
 
data List_filenames;
	set List_filenames;

	where date >= mdy(&db_start_mt,1,&db_start_yr) and
		  date <= mdy(&db_end_mt,1,&db_end_yr);

	Year  		= year(date);
	Month 		= substr(left(100+month(date)),2,2);
	Filename  	= strip(Year)||'_'||strip(Month);
	Keep Filename;
run;

%mend get_filenames;
%get_filenames(&ts_start_yr, &ts_start_mt, &ts_end_yr, &ts_end_mt);


/*1.2. Create a Macro Variable containing all these names					*/
data _null_;
	length allvars $4000;
	retain allvars ' ';
	set list_filenames end=eof;
	allvars = strip(allvars)||' '||left(filename);

	if eof then call symput('varlist', allvars);
run;

/*1.3. Count the number of names (used when looping)						*/
%macro wordcount(list);
	%local count;
	%let 	count = 0; 
	%do %while (%qscan(&list,&count+1, %str( )) ne %str());
		%let count = %eval(&count+1);
	%end;

	&count
%mend wordcount;
%let list_count = %wordcount(&varlist);



*****************************************************************************
STEP 2: PULL DATA FROM OPTION_PRICE DATAFILES.
 - Aligns 1st and Last trade data for each option contract expiring in the
   same month
		- Weeds out weekly options
 		- Select option with more volume/OI between 1m and long-dated options
		  expiring on same date
 - Macro name: pull_OP_data2
 - Output dataset: Data_OP_contracts
****************************************************************************;

%macro pull_OP_data2();

*Used the initialise the Master Dataset on the first run of the macro;
%let flag_createdb  = 1;		

* Loop over monthly datasets, to create a Master Dataset;
%do i = 61 %to &list_count;
	%let filename = %scan(&varlist,&i);	*Default delimiter is blank;
	%put Loop: &i;

	*Use Filename to get End-Of-Month date in DateTime value format;
	%let EOM = %sysfunc(DHMS(%eval(%sysfunc(intnx(month,%sysfunc(inputn(%substr(&Filename,6,2)/01/%substr(&Filename,1,4),MMDDYY10.)),1))-1),0,0,0));

	*************************************************************
	* Identify 1st trade date and Main expiry date				*
	*************************************************************;
	proc sql noprint;

		*FirstTrade_Date = 1st Trade Day of the month;
		select min(Date) format=20.
		into: FirstTrade_Date
		from &dirname.&db_name1.&filename
		;

		*Use contracts from FirstTrade Day to identify the proper
		 monthly exiration date;
		create table temp_expiration as
		select Expiration, count(*) as Ct
		from  &dirname.&db_name1.&filename
		where Date = &FirstTrade_Date and Expiration < &EOM
		group by Expiration
		;					

		*Expiration date with highest volume of contracts would be 
		 primary (Weeklys wont have as many contracts);
		select Expiration format=20.
		into :MonthExpiry_Date
		from temp_expiration as t1
			inner join (select max(ct) as MaxCt
						from temp_expiration) as t2
				on t1.Ct = t2.MaxCT
		;quit;

	/*	%put %sysfunc(putn(&FirstTrade_Date, datetime20.));*/
	/*	%put %sysfunc(putn(&MonthExpiry_Date, datetime20.));*/


	*************************************************************
	*Read monthly data file with daily data on select contracts *
	*************************************************************;
	data Monthly_datafile;
		set &dirname.&db_name1.&filename;
		
		*Same month expiration;
		where Expiration = &MonthExpiry_Date and SpecialSettlement = '0';
			*Special settlement flag value of E means – The option has a 
	   		non-standard expiration date. This is usually due to an error 
			in the historical data which has not yet been researched and fixed;
			*Special settlement flag value of 1 means - some sort of non-standard 
	  		settlement (refer to examples);

		*Date formatting;
		Date			= datepart(Date);		format Date MMDDYY8.;
		Expiration		= datepart(Expiration);	format Expiration MMDDYY8.;

		*Renaming;
		format ImpliedVolatility 7.4 Delta 7.4 Gamma 7.4 Vega 7.4 Theta 7.4;
		rename OpenInterest 		= OI;			rename ImpliedVolatility 	= IV;		
		rename AdjustmentFactor	 	= AF;			
		rename Date					= TradeDate;

		*Adjusting Strike, Bid, Offer;
		format Bid 7.4 Offer 7.4 StrikeAdj 10.2;
		StrikeAdj	= (Strike*AdjustmentFactor)/1000;
		Bid			=  BestBid;
		Offer		=  BestOffer;

		Drop LastTradeDate BestBid BestOffer SpecialSettlement Gamma Vega Theta SymbolFlag;
	run;


	
	*************************************************************
	*Create flags to identify OptionIDS with >1 AF and SS of 1	*
	*************************************************************;
	*Create list of all OptionIDs which go into Special Settlement;
	proc sql;
		create table temp_list_SSflag as
		select distinct OptionID, 1 as SSFlag
		from &dirname.&db_name1.&filename
		where Expiration = &MonthExpiry_Date and SpecialSettlement = '1'
	;quit;

	*Create table to indicate in case AF (for an OptionID) changes during the month;
	proc sql;
		create table temp_table_OptionID_AF as 
		select OptionID, count(distinct AF) as AFCount
		from Monthly_datafile
		group by OptionID
	;quit;


	*************************************************************
	*Option Contracts available on Day 1						*
	*************************************************************;
	*Option contracts on FirstTrade_Date with same-month expiry + Flags created above;
	proc sql;
		create table Temp_Day1 as 
		select t1.*, t2.AFcount, t3.SSflag
		from Monthly_datafile (where=(TradeDate	= datepart(&FirstTrade_Date)))as t1
		left join temp_table_OptionID_AF as t2			on t1.OptionID = t2.OptionID
		left join temp_list_SSflag	as t3				on t1.OptionID = t3.OptionID
	;quit;


	*Four characteristics uniquely identify an OptionContract - SecurityID (S), Strike (K), 
	 Option Type (P/C), Expiration (already fixed to MonthExpiry_Date).
 	*We may have a situation where a 6mnth option and 1mnth option are both expiring in
	 this month. All 4 characteristics will be same for such option contracts.
	 When multiple options with same characteristics exist, we prefer options with (in that order):
	 1) Highest Volume 
	 2) Highest Open Interest;
	
						/* DEBUG - TO CHECK WHAT CASES GET DROPPED IN NEXT STEP
						proc sql;
								create table test0 as
								select *
								from Temp_Day1
								group by SecurityID ,CallPut ,Strike 
								having count(*) >1
						;quit;
						*/
	proc sort data=Temp_Day1         ; by SecurityID CallPut Strike descending Volume descending OI; run;
	proc sort data=Temp_Day1 nodupkey; by SecurityID CallPut Strike 				 			   ; run;


	*************************************************************
	*Identify and separate cases of non-unique OptionID			*
	*************************************************************;
	*Identify if any duplicate OptionIDs exist in the monthly datefile;
	proc sql;
		create table Temp_OptionID_duplicate as
		select distinct OptionID
		from Monthly_datafile
		group by OptionID, TradeDate
		having count(*)>1
	;quit;
	
	*Split 1st day trade data into 2 categories (both will require different merge logic);
	proc sql;
		/*LEFT JOIN WITH EXCLUSION*/
		create table Temp1_Day1_nodup as
		select t1.*
		from Temp_Day1 as t1
		left join Temp_OptionID_duplicate as t2
			on t1.OptionID = t2.OptionID
		where t2.OptionID is NULL
		;

		/*INNER JOIN*/
		create table Temp1_Day1_dup as
		select t1.*
		from Temp_Day1 as t1
		inner join Temp_OptionID_duplicate as t2
			on t1.OptionID = t2.OptionID
	;quit;


	*************************************************************
	*Get Implied Delta value from last day (previous month)		*
	*************************************************************;
	%let Lastfilename = %scan(&varlist,%eval(&i-1));
	Proc sql;
		create table Last_month_datafile as 
		select OptionID, Symbol, Delta, Volume
		from &dirname.&db_name1.&Lastfilename as t1
		inner join (select max(Date) as date from &dirname.&db_name1.&Lastfilename) as t2
			on t1.Date = t2.Date
	;quit;

	Proc sql;
		create table List_Delta as 
		select OptionID, Symbol, Delta, Volume
		from Last_month_datafile
		order by OptionID
	;quit;

	proc sql;
		create table Temp_Day1_nodup as
		select t1.*, t2.Delta as LastDelta format=7.4, t2.Volume as LastVolume
		from Temp1_Day1_nodup as t1
		left join List_Delta as t2
			on t1.OptionID = t2.OptionID
	;quit;

	proc sql;
		create table Temp_Day1_dup as
		select t1.*, t2.Delta as LastDelta format=7.4, t2.Volume as LastVolume
		from Temp1_Day1_dup as t1
		left join List_Delta as t2
			on t1.OptionID = t2.OptionID
			and t1.Symbol  = t2.Symbol
	;quit;


	*************************************************************
	*Keep last available data for each OptionID in the month	*
	*************************************************************;
	*Case I - Based on OptionID;
	proc sort data=Monthly_datafile; by OptionID TradeDate; run;
	data Temp_LastTrade_1;	set Monthly_datafile;	
		by OptionID TradeDate;	
		if last.OptionID;	
	run;

	*Case II - Based on OptionID & StrikeAdj;
	proc sort data=Monthly_datafile; by OptionID StrikeAdj TradeDate; run;
	data Temp_LastTrade_2;	set Monthly_datafile; 	
		by  OptionID StrikeAdj TradeDate;
		if last.OptionID or  last.StrikeAdj;
	run;

	*Case III - Based on OptionID, Symbol & StrikeAdj;
	proc sort data=Monthly_datafile; by OptionID Symbol StrikeAdj TradeDate; run;
	data Temp_LastTrade_3;	set Monthly_datafile; 		 
		by OptionID Symbol StrikeAdj TradeDate;
		if last.OptionID or last.Symbol or last.StrikeAdj;
	run;


	*************************************************************
	*Merge 1st and Last day data (3 cases)						*
	*************************************************************;
	*Case I - OptionIDs with constant value of AdjustmentFactor;
	proc sql;
		create table Temp_Merge1 as 
		select 	t1.*,
				t2.Symbol as Symbol1, 			t2.AF as AF1,					t2.Strike as Strike1,
				t2.TradeDate as ExitDate1, 		t2.StrikeAdj as StrikeAdj1, 	t2.Bid as Bid1,
				t2.Offer as Offer1, 			t2.Volume as Volume1, 			t2.OI as OI1,
				t2.IV as IV1,					t2.Delta as Delta1
		from TEMP_DAY1_NODUP (where=(AFCount = 1)) as t1
			
			/* After entering the contract there may have one of the following:
			  - Name Change (Symbol changes) or 
		      - M&A (SecurityID and Symbol changes) or
			  - Rights Issue or Warrants Issue
			  - Special Dividend (Strike changes, Symbol change depends on nomenclature or
			  Therefore, we cannot match over SecurityID, Symbol or Strike 	*/
		left join Temp_LastTrade_1 as t2
			on  t1.OptionID 	= t2.OptionID	
			and t1.TradeDate 	< t2.TradeDate	
			and t1.CallPut 		= t2.CallPut					
		order by SecurityID, CallPut, Strike
	;quit;

	*Case II - OptionIDs with some Adjustment (change in AdjustmentFactor);	
	proc sql;
		create table Temp_Merge2 as 
		select 	t1.*, 
				t2.Symbol as Symbol1, 			t2.AF as AF1,					t2.Strike as Strike1,
				t2.TradeDate as ExitDate1, 		t2.StrikeAdj as StrikeAdj1, 	t2.Bid as Bid1, 				
				t2.Offer as Offer1, 			t2.Volume as Volume1, 			t2.OI as OI1,
				t2.IV as IV1,					t2.Delta as Delta1
		from TEMP_DAY1_NODUP (where=(AFCount > 1)) as t1
			
			/*After entering the contract there has been an adjustment, 
		      such as Split    */
		left join Temp_LastTrade_2 as t2	
			on t1.OptionID 	= t2.OptionID	
			and t1.TradeDate 	< t2.TradeDate	
			and t1.CallPut 		= t2.CallPut
			and t1.StrikeAdj    = t2.StrikeAdj
		order by SecurityID, CallPut, Strike
	;quit;

	*Case III - OptionIDs tagged to two symbols. We impose harder set of merge 
	            conditions, and keep only sure-shot correct observations;	
	proc sql;
		create table Temp_Merge3 as 
		select 	t1.*, 
				t2.Symbol as Symbol1, 			t2.AF as AF1,					t2.Strike as Strike1,
				t2.TradeDate as ExitDate1, 		t2.StrikeAdj as StrikeAdj1, 	t2.Bid as Bid1,
				t2.Offer as Offer1, 			t2.Volume as Volume1, 			t2.OI as OI1,
				t2.IV as IV1,					t2.Delta as Delta1
		from TEMP_DAY1_DUP as t1
		left join Temp_LastTrade_3 as t2
			on  t1.OptionID 	= t2.OptionID	
			and t1.Symbol		= t2.Symbol
			and t1.StrikeAdj    = t2.StrikeAdj
			and t1.CallPut 		= t2.CallPut
			and t1.SecurityID	= t2.SecurityID
			and t1.TradeDate 	< t2.TradeDate	
		order by SecurityID, CallPut, Strike
	;quit;


	*************************************************************
	*Merge subsets to create the final montly file 				*
	*************************************************************;

	data Temp_Merge;
		set Temp_Merge1 Temp_Merge2 Temp_Merge3;
	run;

			/*FOR DEBUGGING
			proc sql;	create table test0a as	select *	from Temp_Merge		where Symbol ~= Symbol1 and ~missing(Symbol1)			;quit;
			proc sql;	create table test0b as	select *	from Temp_Merge		where AF ~= AF1 and ~missing(AF1)						;quit;
			proc sql;	create table test1 as	select *	from Temp_Merge		where missing(ExitDate1)								;quit;
			proc sql;	create table test2 as	select *	from Temp_Merge		where OptionID = 101131432								;quit;
			proc sql;	create table test3 as	select *	from &dirname.&db_name1.&filename where OptionId = 11936796	order by Date	;quit;
			*/

	* Append monthly files;;
	%if (&flag_createdb = 1 ) %then %do;
		data Data_OP_contracts;	set Temp_Merge;		run;
		data Debug;				set Temp_Day1_dup;	run;
		%let flag_createdb = 0; 
	%end;
	%else %do;								
		proc append base= Data_OP_contracts data=Temp_Merge; run;		
		proc append base= Debug			    data=Temp_Day1_dup; run;
	%end;

*End Loop;
%end;


	*Replace missing values with symbol;
	data Data_OP_contracts; set Data_OP_contracts;
		if (IV    < -99.9) then IV    =.;			if (IV1    < -99.9) then IV1    =.;		
		if (Delta < -99.9) then Delta =.;			if (Delta1 < -99.9) then Delta1 =.;
		if (LastDelta < -99.9) then LastDelta =.;

				/*	if (Gamma < -99.9) then Gamma =.;		
					if (Vega  < -99.9) then Vega  =.;	
					if (Theta < -99.9) then Theta =.;*/
	run;

	*Arrange Columns;
	data Data_OP_contracts; 
		Retain 	SecurityID TradeDate ExitDate1 Symbol Symbol1 AFCount SymbolFlag OptionID CallPut Expiration SSflag AF AF1
			    Strike Strike1 StrikeAdj StrikeAdj1 IV IV1 LastDelta Delta Delta1 LastVolume  
				Bid  Offer  Volume  OI 
				Bid1 Offer1 Volume1 OI1 
				;
		set Data_OP_contracts;
	run;

	*Delete temporary datasets;
	proc datasets noprint; 	delete Temp: Monthly: Last_month_datafile List_Delta; 	run;

%mend pull_OP_data2;

%pull_OP_data2;
/*
Total observations:	8277585

*<4k cases with no exit date;
	proc sql;
		select count(*)	from Data_OP_contracts	where missing(ExitDate1)
	;quit;

*<1% data has special settlement;
	proc sql;
		select nmiss(SSFlag)/count(*) from Data_OP_contracts
	;quit;
*/

*Find the potential last trade date for each expiration.
 Stock Options expire on Saturday after third Friday of the month
 so Last Trade day should be either Friday or Thursday (in case Friday is an Exchange holiday).;
proc sql;
	create table List_ExitDate as 
	select Expiration, weekday(Expiration) as Weekday, max(exitdate1) as LastTradeDtExp format=mmddyy8., Expiration - max(exitdate1) as Gap
	from Data_OP_contracts
	group by Expiration
;quit;

proc sql;
	create table Data_OP_Contracts1 as 
	select t1.*, t2.LastTradeDtExp
	from Data_OP_contracts as t1
	left join List_ExitDate as t2
		on t1.Expiration = t2.Expiration
;quit;

*Create a flag variable, in case Contracts does not trade upto LastTradeDate;
data Data_OP_Contracts1; set Data_OP_Contracts1;
	if ExitDate1 < LastTradeDtExp then EarlyExitFlag = 1 ; else EarlyExitFlag = 0;
run;

*Save Dataset;
data DBfolder.Data_OP_contracts; set Data_OP_Contracts1; run;




*****************************************************************************
STEP 3: MERGE OPTION PRICE DATA WITH CRSP PRICES
3.1. Add Header Information From SECURITY_NAME (CUSIPs) and LINKTABLE (PERMNOs)
3.2. Add EOM and LastTradeDate stock price
****************************************************************************;

*****************************************************************************
PRELIMINARIES
****************************************************************************;

* Run Get_Linktable program - to get Linktable & CRSP_Meta datafiles;
%include '\\dev-sasbi101\temp\Gupta\Code\Get_LinkingTable_J23.sas';
 *Note: Make sure the two datasets are loaded in the Work folder;

* Period for which data is to be read WITH sufficient buffer (+/- 3yr). 
  Includes both end points. Limited period helps with faster execution;
%let start_yr			= 1993;				*USER INPUT;
%let end_yr				= 2014;				*USER INPUT;
%let buffer				= 22;				*Any integer > length of time series in years;
%let Year_range			= &start_yr and &end_yr;	

* CRSP monthly file (RA's cleaned version. However, retains all SHRCD data);
%let raw_CRSP 			= crsp_201505all;	

* CRSP daily file (RA's cleaned version. Original available at \\irv-cvmedia02\research\Constituent Data\Daily CRSP\);
%let raw_CRSP_daily		= CRSPDataDaily_MC;	


/*3.0. Load WorkDB															*/
*Load WorkDB (underlying contracts data from OptionMetrics);
data WorkDB; set DBfolder.Data_OP_contracts; run;


/*3.1. Add Header Information From SECURITY_NAME and LINKTABLE				*/
*Prepare SECURITY_NAME data file;
data Security_name;
	set IvyDB.Security_name;
	date = datepart(date);	format date MMDDYY8.;
run;

*Using date variables, create FROM and TO variables;
proc sort data =  Security_name; by SecurityID descending date; run;
data Security_name;	set Security_name;
	by SecurityID;
	FROM = date;	
	TO   = lag(date)-1;		

	format FROM MMDDYY8.  TO MMDDYY8.;
	if first.SecurityID then TO = mdy(2,2,2222); 
run;

proc sort data =  Security_name; by SecurityID date; run;
data Security_name;	set Security_name;
	by SecurityID date;
	if first.SecurityID then FROM = mdy(12,31,1995);
run;

*Merge WORKDB with SECURITY_NAME dataset;
proc sql;
	create table Temp_Merge1 as
	select 	t1.*, 
		 	t2.CUSIP, t2.Ticker, t2.IssuerDescription, 
			t2.IssueDescription, t2.Class
	from WorkDB as t1
		left join Security_name as t2
		on t1.SecurityID = t2.SecurityID
			and t1.TradeDate >= t2.FROM 
			and t1.TradeDate <= t2.TO		
;
	title 'Ideally we should get 100% match';
	select nmiss(CUSIP) as CUSIP_missing, count(*) as TotalObs
	from Temp_Merge1
;
quit;

*Create Year & Month;
data Temp_Merge1; set Temp_Merge1;
	year = year(TradeDate);
	month = month(TradeDate);
run;

*Merge PERMNO from Linktable;
proc sql;
	*LEFT JOIN;
	create table Temp_Merge2 as
	select 	t1.*,
		   	t2.PERMNO, t2.PERMCO, t2.LinkPrim, t2.WS_ID, t2.GVKEY,
			t2.Comnam, t2.Ticker as CRSP_Ticker, t2.ShrCd, t2.EXCHCD
	from Temp_Merge1 as t1
	left join Linktable as t2
		on 	t1.CUSIP = t2.CRSP_CUSIP
		and	t1.YEAR  = t2.YEAR
		and t1.MONTH = t2.MONTH
	order by SecurityID, TradeDate;

	*Match ratio - 92%;
	title 'Merge Linktable match ratio?';
	select count(CASE WHEN PERMNO is not NULL THEN 1 END) as Matched,
		   count(CASE WHEN PERMNO is     NULL THEN 1 END) as Unmatched,
		   count(CASE WHEN PERMNO is not NULL THEN 1 END)/count(*) as MatchRatio
	from Temp_Merge2;
quit;

*Save;
data DBfolder.Data_OP_Merged; set Temp_Merge2; run;

*Delete intermediata datasets;
proc datasets noprint; delete WorkDB Temp: Security_Name Linktable CRSP_Meta ; run;



/*3.2. Add EOM and LastTradeDate stock price								*/
*Load WorkDB;
data WorkDB; set DBfolder.Data_OP_Merged; run;

*Load CRSP Monthly Dataset;
data CRSP_monthly;	
	set DBfolder.&raw_CRSP; 
	where Year between &Year_range;
	Keep Permno Year Month StockPrice;
run;

*Create Last_year/ Last_month variables since CRSP prices are end of month
 while Trade Date is start of month;
data WorkDB; set WorkDB;
	last_month = month(intnx('month',TradeDate,-1));
	last_year  =  year(intnx('month',TradeDate,-1));
run;

*Merge Stock Prices with WorkDB;
proc sql;
	create table WorkDB1 as 
	select t1.*, t2.StockPrice as TradeDatePrc_crsp
	from WorkDB as t1
	left join CRSP_monthly as t2
		on t1.PERMNO = t2.PERMNO
		and t1.Last_Year = t2.Year
		and t1.Last_Month = t2.Month
;quit;

		/*
		*~9k cases where Last month's EOM price is missing despite PERMNO;
		proc sql;
			create table test as 
			select *
			from WorkDB1
			where ~missing(PERMNO) and missing(TradeDatePrc_crsp)
		;quit;
		*/


*Load CRSP Daily Dataset;
data CRSP_Daily;
	set DBfolder.&raw_CRSP_daily;
	where year(date) between &Year_range;
	PERMNO = ENTITY*1;	format PERMNO 11.;
	rename Prcprev = StockPrice;
run;

*Clean variables;
data CRSP_Daily; 
	retain PERMNO; set CRSP_Daily;

	if ret < -1 then delete;
	if tvol = -99 then tvol = .;
	if adjvol = -99 then adjvol = .;
	
	StockPrice = abs(StockPrice);
	AdjPrc	   = abs(AdjPrc);
	format 	StockPrice AdjPrc 7.2 Ret 7.4;

	drop ENTITY;
run;


*Add Stockprice (unadjusted) for the last trading day before contract expiration;
proc sql;
	create table WorkDB2 as 
	select t1.*, t2.StockPrice as ExpiryPrc_crsp
	from WorkDB1 as t1
	left join CRSP_Daily as t2
		on t1.PERMNO 			= t2.PERMNO
		and t1.LastTradeDtExp   = t2.Date
;quit;

		/*
		*<6k observations with missing price in CRSP_Daily
		 Cases of Acquisition (MGM), Privatization (Encore Med) etc.;
			proc sql;
				create table test as
				select *
				from WorkDB2
				where ~missing(PERMNO) and missing(ExpiryPrc_crsp)
			;quit;
		*/

*Save;
data DBfolder.Data_OP_CRSP_Merged; set WorkDB2; run;

*Delete intermediate datesets;
proc datasets noprint; delete Workdb: CRSP_:; run;


*****************************************************************************
STEP 4: ADD ALPHA SIGNAL
4.1: Create Moneyness Variable
4.2: Merge Alpha Values
****************************************************************************;

/*4.1: Create Moneyness Variable											*/
data WorkDB; 
	Retain Year Month SecurityID PERMNO Comnam CRSP_Ticker;
	set DBfolder.Data_OP_CRSP_Merged;
run;

*Moneyness variable;
data WorkDB; set WorkDB;
	format Moneyness 7.4;
	if (TradeDatePrc_Crsp>0) then Moneyness = (Strike/1000)/TradeDatePrc_Crsp; else Moneyness =.;;
	*We use Strike/1000 in place of StrikeAdj, because stock may have split after the 
	 contract was written and before the trade date (AF ne 1). In that case raw stock
	 price would reflect the split, as would raw strike (used above).;
run;

		/*	
		*We use StockPrice instead of AdjPrice to get the actual historical price 
   		 on that day, e.g. MSFT, actual price was 47, while adjusted price would be 26;
		proc sql;
			select *
			from WorkDB
			where SecurityID = 107525 and Year = 2003 and Month = 2 and AFcount = 2
		;quit;

		*Cases of extreme money-ness values, are instances of long-dated option, 
		 where the underlying has moved by a lot, e.g. Sun Microsystems 100x fall;
		proc sql;
			create table test as 
			select *
			from WorkDB
			where Moneyness >10
		;quit;
		*/


/*4.2: Merge Alpha Values													*/
*Load Table_Alpha;
data Table_Alpha;
	set DBFolder.Table_Alpha;
run;

*Merge Alpha values;
proc sql;
	create table WorkDB1 as 
	select t1.*, t2.Alpha
	from WorkDB as t1
		left join Table_Alpha as t2
		on t1.PERMNO = t2.PERMNO
		and t1.Last_Year = t2.Year
		and t1.Last_Month = t2.Month
;quit;
	/* ~1000 obs comprising <15 firms that dont have alpha values
		Seem like cases of delisting (Evergreen, Power Integration), Relisting (HealthSouth - after restructuring, Pilgrims Pride - reorganization), 
        First time Listing (Huntington Ingalls - after spin-off), Merger (Lin Media LLC) etc.;

		proc sql;
			create table test as select *
			from WorkDB1
			where missing(Alpha) and ~missing(PERMNO) and SHRCD in (10,11)
		;quit;
	*/

*Save;
data DBFolder.Data_OP_allmerged; set WorkDB1; run;



*****************************************************************************
STEP 5: RUN BACKTEST
5.1: Load dataset - filter for relevant observations
5.2: Calculate Payoff on expiry
5.3: Delete cases of Corporate Actions
5.4: Identify contracts with target delta and run backtest
****************************************************************************;

/*5.1: Load dataset - filter for relevant observations						*/
data WorkDB; 
	set DBFolder.Data_OP_allmerged; 

	where 
		SHRCD in (10,11) 			/* inherently also filters for ~missing(PERMNO)	*/
		and ~missing(Alpha)			/* trade signal is non-missing					*/
		and OI >=100				/* option contract should have minimum pen interest of 100	*/
		and Bid >0 					/* quoted price for Bid should be >0 			*/
		and Bid < Offer				/* cases were found with this violation			*/
		and abs(Bid-Offer) > 0.04 	/*Tick size is 0.05 for options under $3, and otherwise $.01*/
	;

	*Drop redundant columns;
	drop Ticker IssuerDescription IssueDescription Class PERMCO LinkPrim WS_ID GVKEY SHRCD EXCHCD CUSIP;
run;
	/* 
	*~600k observations where quoted Bid = 0 on trade date;
	data Test; set DBFolder.Data_OP_allmerged; where SHRCD in (10,11) and ~missing(Alpha) and OI >= 100 and Bid = 0; run;

	*While at this stage, it seems we are loosing a lot of data due to the 
	 condition Bid >0, it is not so. Later when we filter for ~missing(delta) and 
	 for 1 option contract within the target delta range, we end up deleting most
	 of the Bid=0 cases.
	*/

	/*
	 *Note: The above dataset can be split into following mutually exclusive buckets;
		proc sql;	create table Test1 as	select *	from WorkDB		where SSFlag = 1										;quit;
		proc sql;	create table Test2 as	select *	from WorkDB		where SSFlag = . and AFcount>1	and EarlyExitFlag=0		;quit;
		proc sql;	create table Test3 as	select *	from WorkDB		where SSFlag = . and AFcount>1	and EarlyExitFlag=1		;quit;
		proc sql;	create table Test4 as	select *	from WorkDB		where SSFlag = . and AFcount=1	and EarlyExitFlag=0		;quit;
		proc sql;	create table Test5 as	select *	from WorkDB		where SSFlag = . and AFcount=1	and EarlyExitFlag=1		;quit;

	 *Except the first case, where contracts go into special settlement, we can calculate payoff using stock price for all;

	 *Check: All cases where security goes into Special Settlement are those with AFcount = 1;
		proc sql;	create table Test as	select distinct AFcount 	from WorkDB		where SSflag=1							;quit;

	 *Plot histogram of delta in the data;
		proc univariate data = WorkDB ; var delta;	histogram;	run;
	*/

/*5.2: Calculate Payoff on expiry											*/

*Calculate Payoff at LastTradeDtExp using stock price and strike price;
data WorkDB; set WorkDB;

	*Payoff (with number of underlyings adjusted relative to starting number AF);
	if ~missing(ExpiryPrc_crsp) and CallPut = 'P' 
				then Payoff = max(0, Strike1/1000   - ExpiryPrc_crsp)*(AF1/AF);
	if ~missing(ExpiryPrc_crsp) and CallPut = 'C' 
				then Payoff = max(0, ExpiryPrc_crsp - Strike1/1000  )*(AF1/AF);

	*Cases where option contract went into special settlement;
	if SSFlag = 1 or missing(ExpiryPrc_crsp) then Payoff = .;

	format Payoff 7.4;
run;


/*5.3: Delete cases of Corporate Actions									*/

	/*
	*~1200 observations where Payoff is missing, despite the contract not going 
    into special settlement Bankruptcy (Aledphia), Split (Motorola Solutions), 
    Cash Distribution (Hollyfrontier corp; not caught as SSFlag since record date was)

		data test;	set WorkDB;	where missing(Payoff) and missing(SSflag);	run;
	
	*Hollyfrontier appears to be a case of incorrect data in OM database - OCC shows co. distributed cash and Strike should
	 have reduced by a small amount;

		data test; set IvyDb.Option_Price_2012_03;	where OptionID = 63302174;	run;

	 *Since these Corporate actions are known in advance, these trades can be avoided. So we can take them out of our dataset;
	*/

	data WorkDB; set WorkDB;
		if missing(SSflag) and missing(Payoff) then delete;
	run;


	/*
	*~300 cases of Special Settlement, where contract goes into special settlement after Trade Day itself.
	 Hence no Bid1/Offer1 exist. Since these instances too would be known in advance, we can take them off our dataset;

	 data Test; set WorkDB; where SSflag = 1 and missing(Bid1); run;
	*/
	data WorkDB; set WorkDB;
		if SSflag = 1 and missing(Bid1) then delete;
	run;



*Calculate Returns that a Price Taker would make (ReturnB = Long Position. ReturnS = Short Position) based on Payoff;
data WorkDB1; set WorkDB;

	if ~missing(Payoff) then do;
		ReturnB  =  (Payoff - Offer)/Offer;
		ReturnS  = -(Payoff - Bid)/Bid;
	end;

	if missing(Payoff) then do;
		ReturnB  =  (Bid1 - Offer)/Offer;
		ReturnS  = -(Offer1 - Bid)/Bid;
	end;

		ReturnMid  = ((Bid1 + Offer1)/(Bid + Offer) - 1);
		ReturnMMS  = -(Bid1 - Offer)/Offer;		/*approx Opposite of ReturnB. Price taker exits at payoff*/
		ReturnMMB  =  (Offer1 - Bid)/Bid;		/*approx Opposite of ReturnS. Price taker exits at payoff*/
		ReturnB2   = (Bid1 - Offer)/Offer;		/*price taker gives full spread*/
		ReturnS2   = -(Offer1 - Bid)/Bid;		/*price taker gives full spread*/

		StockRetDelta   = (-1)*LastDelta*( ( (ExpiryPrc_CRSP*AF1)/(TradeDatePrc_CRSP*AF) )-1);
		RetDeltaHedged	= ReturnMid + StockRetDelta;
	format ReturnB ReturnS ReturnMid ReturnMMB ReturnMMS ReturnB2 ReturnS2 StockRetDelta RetDeltaHedged 7.4;
run;


/*5.4: Identify contracts with target delta and run backtest				*/
*Keep contracts with target Delta value;
%let Delta_Low  = 0.30; 
%let Delta_High = 0.40;

*Only include contracts with non-missing Delta value;
data Subset_All; set WorkDB1;	where ~missing(LastDelta);	run;

data Subset_Call Subset_Put;
	set Subset_All;
	if LastDelta >0 and abs(LastDelta) >= &Delta_Low and abs(LastDelta) <= &Delta_High then output Subset_Call;
	if LastDelta <0 and abs(LastDelta) >= &Delta_Low and abs(LastDelta) <= &Delta_High then output Subset_Put;
run;

*Keep only one contract (with higher abs(Delta)) for each SecurityID for a given TradeDate;
proc sort data=Subset_Call; by Year Month SecurityID LastDelta; run;
proc sort data=Subset_Put ; by Year Month SecurityID LastDelta; run;
data Subset_Call; set Subset_Call;	by Year Month SecurityID;	if last.SecurityID;		run;
data Subset_Put; set Subset_Put;	by Year Month SecurityID;	if first.SecurityID;	run;


*****************************************************************************
MACRO TO CREATE TIME SERIES OF RETURNS
****************************************************************************;

%Macro Get_TS (DB = , Var = , Name = , Grp =, Lag =);
/*For Debugging
%let DB = Subset_Call;	%let Var = Alpha;	%let Name = Call;		
%let Grp= 5;			%let Lag = 0;
*/

	*Sort & Rank inputdb;
	proc sort  data=&DB; by Year Month &Var; run;

	proc rank data=&DB out=Temp_DB groups = &Grp;
		var &Var;
		by Year Month;
		ranks &Var._rk;
	run;

	proc sql;
		create table Temp_alpha as 
		select &Var._rk, MEAN(Alpha) as AvgAlpha
		from Temp_DB
		group by &Var._rk
	;quit;


	*Calculate Equal Weights for monthly portfolios (exclude stocks with missing ranks);
	proc sql;
		create table Temp_Weights as
		select Year, Month, &Var._rk, 1/count(*) as Wt, count(*) as Count
		from Temp_DB
		group by Year, Month, &Var._rk
	;quit;

	*Merge weights (INNER JOIN);
	proc sql;
		create table Temp_DB2 as
		select t1.*, t2.Wt
		from Temp_DB as t1
			inner join Temp_Weights as t2
			on  t1.&Var._rk	 =	t2.&Var._rk
			and t1.Year		 =	t2.Year
			and t1.Month	 =	t2.Month;
	quit;


	*Calculate Returns Time Series;
	proc sql;
		create table Temp_TS as
		select Year, Month, &Var._rk, 
			   count(Wt) as OptionContracts, 
			   sum(ReturnB*Wt) as PortRetB,
			   sum(ReturnS*Wt) as PortRetS,
			   sum(ReturnMid*Wt) as PortRetMid,
			   sum(ReturnMMS*Wt) as PortRetMMS,
			   sum(ReturnMMB*Wt) as PortRetMMB,
			   sum(ReturnB2*Wt) as PortRetB2,
			   sum(ReturnS2*Wt) as PortRetS2,
			   sum(RetDeltaHedged*Wt) as PortRetDeltaHedged,
			   sum(StockRetDelta*Wt) as PortRetStockDelta

		from Temp_DB2
		group by Year, Month, &Var._rk
		order by Year, Month, &Var._rk
	;quit;


	*Mean Return of portfolios;
	proc sql;
		create table Temp_Table as 
		select &Var._rk, Mean(PortRetB)   as RetBuy  format=7.4, 
						 Mean(PortRetS)   as RetSell format=7.4,
						 Mean(PortRetMid) as RetMid	 format=7.4,
						 Mean(PortRetMMS)  as RetMMS   format=7.4,
						 Mean(PortRetMMB)  as RetMMB   format=7.4,
						 Mean(PortRetB2)  as RetB2   format=7.4,
						 Mean(PortRetS2)  as RetS2  format=7.4,
						 Mean(PortRetDeltaHedged) as RetDeltaHedged format=7.4,
						 Mean(PortRetStockDelta) as RetStockDelta format=7.4
		from Temp_TS
		group by &Var._rk;
	quit;


	proc sql;
		create table  Table_&name as 
		select t1.*, t2.AvgAlpha format=7.4
		from Temp_Table as t1
		left join Temp_alpha as t2
			on t1.Alpha_rk = t2.Alpha_rk
	order by t1.Alpha_rk
	;quit;

	*Delete intermediate datasets;
	proc datasets noprint; delete Temp_:; run;

%mend Get_TS;
	
%Get_TS (DB = Subset_Call, Var = Alpha, Name = Call, Grp = 5, Lag = 6);
%Get_TS (DB = Subset_Put,  Var = Alpha, Name = Put,  Grp = 5, Lag = 6);




/*
*****************************************************************************
PROPERTIES OF OPTIONID: 
- Sample is full time period
- Option contracts are filtered to keep same month expiry contracts only
****************************************************************************;

* How big is the dataset;
* 1,028mn obs overall - 139mn obs for same month expiry;
%macro Loop_check0();
%let flag_createdb  = 1;	
%do i = 1 %to &list_count;
	%let filename = %scan(&varlist,&i);	
	%put &i;

	proc sql noprint;
		create table temp3 as
		select %substr(&Filename,1,4) as Year, %substr(&Filename,6,2) as Month, count(*) as Nobs
		from &dirname.&db_name1.&filename
		where month(date) = month(expiration) and year(date) = year(expiration) and SpecialSettlement='0'
	;quit;

	proc sql noprint;
		create table temp1 as
		select %substr(&Filename,1,4) as Year, %substr(&Filename,6,2) as Month, count(*) as Nobs
		from &dirname.&db_name1.&filename
		where month(date) = month(expiration) and year(date) = year(expiration)
	;quit;

	proc sql noprint;
		create table temp2 as
		select %substr(&Filename,1,4) as Year, %substr(&Filename,6,2) as Month, count(*) as Nobs
		from &dirname.&db_name1.&filename
	;quit;

	proc sql;
		create table temp4 as
		select t1.Year, t1.Month, t3.Nobs as Nobs_SS0, t1.Nobs as Nobs_Sample, t2.Nobs as Nobs_FullData, 
				t3.Nobs/t1.Nobs as SS0_ratio, t1.Nobs/t2.Nobs as SameMonth_ratio
		from temp1 as t1
		left join temp2 as t2
			on t1.Year = t2.Year
			and t1.Month = t2.Month
		left join temp3 as t3
			on t1.Year = t3.Year
			and t1.Month = t3.Month
	;quit;

	*Append monthly files;
	%if (&flag_createdb = 1 ) %then %do;
		data Temp_all_0;		set Temp4;		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;	proc append base= Temp_all_0 data=Temp4; run;	%end;
%end;
%mend Loop_check0;
%Loop_check0;


* Instances where OptionID repeats on a given day;
* 338 observations comprising 11 unique SecurityID;
* In all cases, OptionID is tagged to 2 different symbols of same SecurityID;
%macro Loop_check1();
%let flag_createdb  = 1;	
%do i = 1 %to &list_count;
	%let filename = %scan(&varlist,&i);	
	%put &i;

	proc sql noprint;
		create table temp1 as
		select *
		from &dirname.&db_name1.&filename
		where month(date) = month(expiration) and year(date) = year(expiration)
		group by Date, OptionID
		having count(*)>1
	;quit;

	*Append monthly files;
	%if (&flag_createdb = 1 ) %then %do;
		data Temp_all_1;		set Temp1;		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;	proc append base= Temp_all_1 data=Temp1; run;	%end;
%end;
%mend Loop_check1;
%Loop_check1;


* Instances where OptionID is tagged with >1 SecurityID in the monthly datafile;
* We get 433 unique pairs of SecurityIDs linking to same OptionID;
* Latest Company name for both the SecurityIDs is same i.e. these are cases of M&A;
%macro Loop_check2();
%let flag_createdb  = 1;	
%do i = 1 %to &list_count;
	%let filename = %scan(&varlist,&i);	
	%put &i;

	proc sql noprint;
		create table temp1 as
		select *
		from &dirname.&db_name1.&filename
		where month(date) = month(expiration) and year(date) = year(expiration)
		group by OptionID
		having count(distinct SecurityID)>1
	;quit;

	*Append monthly files;
	%if (&flag_createdb = 1 ) %then %do;
		data Temp_all_2;	set Temp1;		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;	proc append base= Temp_all_2 data=Temp1; run;	%end;
%end;
%mend Loop_check2;
%Loop_check2;
proc sort data=Temp_all_2 out=temp_all_2a nodupkey; by OptionID SecurityID; run;

proc sql;
	create table pairs as
	select t1.OptionID, t1.SecurityID as Sec1, t2.SecurityID as Sec2, t1.Date as Dt1, t2.Date as Dt2
	from temp_all_2a as t1
	inner join temp_all_2a as t2
		on t1.OptionId = t2.OptionId
		and t1.SecurityID>t2.SecurityID
;quit;

proc sort data=Pairs nodupkey; by Sec1 Sec2; run;

data SN_last; set IvyDb.Security_NAME; run;
proc sort data=SN_LAST nodupkey; by SecurityID; run;

proc sql;
	create table Pairs_info as
	select t1.*, t2.IssuerDescription as Sec1_Name, t2.IssuerDescription as Sec2_Name
	from Pairs as t1
	left join SN_LAST as t2
		on t1.Sec1 = t2.SecurityID
	left join SN_LAST as t3
		on t1.Sec2 = t3.SecurityID
;quit;
*/

/*
* Any instance of wrong data, such as Bid > Ask?;
* 750k such observations in the dataset;
%macro Loop_check3();
%let flag_createdb  = 1;	
%do i = 1 %to &list_count;
	%let filename = %scan(&varlist,&i);	
	%put &i;

	proc sql noprint;
		create table temp1 as
		select *
		from &dirname.&db_name1.&filename
		where BestBid > BestOffer
	;quit;

	*Append monthly files;
	%if (&flag_createdb = 1 ) %then %do;
		data Temp_all_3;	set Temp1;		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;	proc append base= Temp_all_3 data=Temp1; run;	%end;
%end;
%mend Loop_check3;
%Loop_check3;
*/

/*
* Any instance of bid-offer spread lower than 0.05;
* 42.5 mn with issues: (1) Same Bid & Offer (2) Spread lower than tick size;
%macro Loop_check4();
%let flag_createdb  = 1;	
%do i = 1 %to &list_count;
	%let filename = %scan(&varlist,&i);	
	%put &i;

	proc sql noprint;
		create table temp1 as
		select *
		from &dirname.&db_name1.&filename
		where abs(BestOffer-BestBid)<0.05
	;quit;

	*Append monthly files;
	%if (&flag_createdb = 1 ) %then %do;
		data Temp_all_4;	set Temp1;		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;	proc append base= Temp_all_4 data=Temp1; run;	%end;
%end;
%mend Loop_check4;
%Loop_check4;
*/

/*
*Can a month have more than two expiration dates?
 YES - Starting October 2005, we see multiple expiration dates in a month.
 This is due to introduction of weekly options;
%macro Loop_check2();
%let flag_createdb  = 1;	

%do i = 1 %to 216;*&list_count;
	%let filename = %scan(&varlist,&i);	*Default delimiter is blank;
	%put &i;

	proc sql noprint;
		create table temp1 as
		select  distinct Expiration, %substr(&filename,1,4) as Year, %substr(&filename,6,2) as Month
		from &dirname.&db_name1.&filename
	;quit;

	%if (&flag_createdb = 1 ) %then %do;
		data Temp_all_2;
			set Temp1;
		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;	proc append base= Temp_all_2 data=Temp1; run;	%end;
%end;

proc sort data=Temp_all_2 nodupkey; by Expiration; run;

data Temp_all_2;	set Temp_all_2;
	Expiration = datepart(Expiration); format Expiration MMDDYY8.;
	Expiry_Y	= year(Expiration);
	Expiry_M	= month(Expiration);
	Expiry_D	= day(Expiration);
run;

proc sql;
	create table test as
	select Expiry_y, Expiry_m, count(*) as ct
	from Temp_all_2
	group by Expiry_y, Expiry_m
	having ct>1
;quit;

%mend Loop_check2;
%Loop_check2;

****************************************************************************;
*Example of symmbol change for an option contract?;
%macro Loop_check4();
%let flag_createdb  = 1;	

%do i = 1 %to 216;*&list_count;
	%let filename = %scan(&varlist,&i);
	%put &i;

	proc sql noprint;
		create table temp1 as
		select *
		from &dirname.&db_name1.&filename
		where OptionID in ( 45643036 )
	;quit; 				*(45643036, 945439154, 45956253, 45412899);


	*Append monthly files;
	%if (&flag_createdb = 1 ) %then %do;
		data Temp_all_4;
			set Temp1;
		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;	proc append base= Temp_all_4 data=Temp1; run;	%end;

%end;
%mend Loop_check4;
%Loop_check4;

****************************************************************************;
*Case of corporate actions, such as M&A in this example. While the SecurityID 
 and Symbol changed, OptionID did not.;
proc sql;
	create table test as
	select *
	from Ivydb.Option_Price_2001_02
	where SecurityID = 102037 and Symbol = 'QBS.BW'
;quit;

*Using OptionID (as primary key) would have worked in this case;
proc sql;
	create table test as
	select *
	from Ivydb.Option_Price_2001_02
	where OptionID = 20019826
;quit;


****************************************************************************;
*Can their be Option Contracts (trading simultaneously) with exactly same 
 characteristics but different Settlement flag?
 Conclusion: YES;
%macro Loop_check5();
%let flag_createdb  = 1;	
%do i = 1 %to 10;
	%let filename = %scan(&varlist,&i);
	%put &i;

	proc sql noprint;
		create table temp1 as
		select *
		from &dirname.&db_name1.&filename
		group by SecurityID, Expiration, Strike, CallPut, Date
		having count(distinct SpecialSettlement)>1
	;quit;

	*Append monthly files;
	%if (&flag_createdb = 1 ) %then %do;
		data Temp_all_5;		set Temp1;		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;	proc append base= Temp_all_5 data=Temp1; run;	%end;

%end;
%mend Loop_check5;
%Loop_check5;

****************************************************************************;

*Can Adjustment Factor also reduce over time? - Checking only for same-month expiry options
 Conclusion: YES (ERROR WITH DATASET) ~2300 cases with same month expiry obs;
%macro Loop_check6();
%let flag_createdb  = 1;	
%do i = 1 %to 216;
	%let filename = %scan(&varlist,&i);
	%put &i;

	proc sql noprint;
		create table temp1 as
		select t1.*, t2.Date as Date2, t2.AdjustmentFactor as AF2
		from &dirname.&db_name1.&filename (drop = ImpliedVolatility Delta Gamma Vega Theta LastTradeDate) as t1
		inner join &dirname.&db_name1.&filename (keep = Symbol OptionID Date AdjustmentFactor) as t2
			on t1.OptionID = t2.OptionID
			and t1.Symbol = t2.Symbol
			and t1.Date < t2.Date
			and t1.AdjustmentFactor > t2.AdjustmentFactor
			and month(t1.date) = month(t1.expiration)
	;quit;

	proc sort data=temp1 nodupkey; by OptionID AdjustmentFactor; run;

	*Append monthly files;
	%if (&flag_createdb = 1 ) %then %do;
		data Temp_all_6;		set Temp1;		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;	proc append base= Temp_all_6 data=Temp1; run;	%end;

%end;
%mend Loop_check6;
%Loop_check6;

data DBFolder.Temp_Check6; set Temp_all_6; run;

proc sql;
	create table test as
	select Date, count(*)
	from Temp_all_6
	group by date
;quit;
****************************************************************************;

*Cases of Special Dividend - Symbol/ Strike changes without any change to Adjutment Factor;
*Check how many such cases exist;

%macro Loop_check7();
%let flag_createdb  = 1;	
%do i = 61 %to 70;
	%let filename = %scan(&varlist,&i);
	%put &i;

	proc sql noprint;
		create table temp1 as
		select *
		from &dirname.&db_name1.&filename
		where SpecialSettlement = '0' 
		group by Symbol
		having count(distinct Strike)>1
	order by Symbol, Date
	;quit;

	*Append monthly files;
	%if (&flag_createdb = 1 ) %then %do;
		data Temp_all_7;		set Temp1;		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;	proc append base= Temp_all_7 data=Temp1; run;	%end;

%end;
%mend Loop_check7;
%Loop_check7;

proc sql;
create table list as
select distinct SecurityID
from Temp_all_7
;quit;

proc sql;
create table list_info as
select t1.*, t2.*
from list as t1
left join ivydb.Security as t2
	on t1.SecurityID = t2.SecurityID
where IndexFlag = '0' and IssueType = '0'
;quit;

proc sql;
	create table test as
	select *
	from Temp_all_7
	where SecurityID in (101535, 103042, 108321, 109090)
;quit;

proc sql;
	select *
	from ivydb.Security_Name
	where SecurityID in (101535, 103042)
;quit;

proc sql;
	select *
	from ivydb.Distribution
	where SecurityID in (101535, 103042)
;quit;
****************************************************************************;

*/