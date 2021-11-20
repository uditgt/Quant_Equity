******************************************************************************
PURPOSE		: 	Create collated datasets by pulling data from monthly datafiles.
				1) VOLATILITY_SURFACE - pulls data for given point(s) on surface
				2) OPTION_PRICE - pulls monthly aggregate volume and OI
INPUT		: 	User Inputs required for VOLATILITY_SURFACE datapull:
					1) Time Period (for the data pull)
					2) Delta(s)    (takes multiple valies in strike dimension)
					3) Days        (takes single value in time dimension)
OUTPUT		:  	DATA_VS
				DATA_OP 		  (monthly volume and open interest)
COMMENTS	:	Primary macros are 'pull_VS_data' and 'pull_OP_data1'. 
				Each macro takes about ~10mins to execute.
AUTHOR		:	UDIT GUPTA (September, 2015)
*****************************************************************************

*****************************************************************************
PRELIMINARIES
****************************************************************************;
* Setting up ODBC connection, and loading the Schema;
libname IVyDB odbc complete='DSN=SQLServerProdN;Database=IVyDB' schema=dbo;

* Location of Dataset folder;
libname DBfolder ' \\dev-sasbi101\temp\Gupta\Datasets ';

* USER INPUTS;
* Time Series to be pulled;
%let ts_start_yr = 1996;
%let ts_start_mt = 1;
%let ts_end_yr   = 2013;		
%let ts_end_mt   = 12;

* Variables defining data to be pulled;
%let delta_pull 	= (50,-50,-20);	*delta values range from +/- 20 to +/-80 in intervals of 5;
%let days_pull 		= (30);			*30,60,91,122,152,182,273,365,547, & 730 calendar days;

* Provide Database dates;
%let dirname		= IvyDB. 				;
%let db_name		= Volatility_Surface_ 	;	*used in pull_VS_data;
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
STEP 2: PULL DATA FROM VOLATILITY_SURFACE DATAFILES.
 - Loop over monthly files to create 1 collated dataset
 - Retains only End-of-Month observations
 - Macro name: pull_VS_data
 - Output dataset: Data_VS
****************************************************************************;
%macro pull_VS_data();

*Used to initialise the Master Dataset on the first run of the macro;
%let flag_createdb  = 1;		

*Loop over monthly datasets, to create a Master Dataset;
%do i = 1 %to &list_count;
	%put Loop: &i;
	%let filename = %scan(&varlist,&i);	*Default delimiter is blank;
	
	proc sql noprint;
		select Max(date) format=20.
		into: EOM_Date 
		from &dirname.&db_name.&filename
	;quit;

	data Monthly_File_EOM;
		set &dirname.&db_name.&filename;
		where Date = &EOM_date and 
			  Delta in &delta_pull and
			  Days  in &days_pull;

		Date = datepart(date);
		format date mmddyy8.;
	run;

	*Append monthly files;
	%if (&flag_createdb = 1 ) %then %do;
		data Data_VS;
			set Monthly_File_EOM;
		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;								
		proc append base= Data_VS data=Monthly_File_EOM; run;	
	%end;

%end;

*Delete temporary datasets;
proc datasets noprint; delete Monthly: ; run;

%mend pull_VS_data;

%pull_VS_data;



*****************************************************************************
STEP 3: PULL DATA FROM OPTION_PRICE DATAFILES.
 - Collates monthly Volume/ Open Interest in each SecurityID, by summing over
   daily data
 - Macro name: pull_OP_data1
 - Output dataset: Data_OP1
****************************************************************************;
%macro pull_OP_data1();

*Used to initialise the Master Dataset on the first run of the macro;
%let flag_createdb  = 1;		

* Loop over monthly datasets, to create a Master Dataset;
%do i = 1 %to &list_count;
	%let filename = %scan(&varlist,&i);	*Default delimiter is blank;
	%put Loop: &i;

	* Read-in the monthly data file;
	proc sql;
		create table Monthly_File as
		select SecurityID, CallPut, 
			   %substr(&filename,1,4) as Year, %substr(&filename,6,2) as Month,
			   sum(Volume) as Volume, sum(OpenInterest) as OI
		from &dirname.&db_name1.&filename
		group by SecurityID, CallPut
	;quit;

	* Append monthly files;;
	%if (&flag_createdb = 1 ) %then %do;
		data Data_OP;
			set Monthly_File;
		run;
		%let flag_createdb = 0; 
	%end;
	%else %do;								
		proc append base= Data_OP data=Monthly_File; run;	
	%end;
%end;

*Delete temporary datasets;
proc datasets noprint; delete Monthly_File; run;

%mend pull_OP_data1;
%pull_OP_data1;


*****************************************************************************
STEP 5: FORMAT DATASETS.
5.1 DATA_VS: Transpose data to wide format and create Delta variables
5.2 DATA_OP1: Transpose data to wide format and create Delta & Ratio variables
5.3 Add Volume and OI from DATA_OP to DATA_VS
5.4 Save datasets to local folder
****************************************************************************;

/*5.1 DATA_VS: Transpose data to wide format and create Delta variables		*/
*Clean missing values;
data Data_VS;
	set Data_VS (drop= ImpliedStrike ImpliedPremium Dispersion);
	Delta = abs(delta);			*CallPut field is sufficient to identify Call/ Puts;
	if ImpliedVolatility <0 then ImpliedVolatility = .;
	rename ImpliedVolatility = VOL;
run;

*Transpose;
proc sort data=Data_VS out=Data_VS; by SecurityID Date; run;

proc transpose data=Data_VS out=Data_VS (drop=_NAME_ _LABEL_) suffix=VOL;
	by SecurityID Date;
	id CALLPUT DELTA;
	var VOL;
run;

proc sort data=Data_VS; by SecurityID Date; run;

*Delta variables;
data Data_VS (DROP = Month_gap);
	set Data_VS;
	by SecurityID Date;
	
	year			= year(date);
	month			= month(date);

	Month_gap 		= month - lag(month);
	C50VOLdelta		= C50VOL - lag(C50VOL);
	P50VOLdelta		= P50VOL - lag(P50VOL);

	if first.SecurityID or Month_gap not in (1,-11)	then do;
		C50VOLdelta	=	.;
		P50VOLdelta	=	.;
	end;

	PdCdDiff		= P50VOLdelta - C50VOLdelta;
run;



/*5.2 DATA_OP: Transpose data to wide format and create Ratio variables		*/
proc sort data=Data_OP out=Data_OP; by SecurityID YEAR MONTH; run;

proc sql;
	create table Data_OP1 as
	select t1.SecurityID, t1.Year, t1.Month, 
		   t1.Volume as C_Volume, t1.OI as C_OI,
		   t2.Volume as P_Volume, t2.OI as P_OI
	from Data_OP as t1
		inner join Data_OP as t2
		on t1.SecurityID	=	t2.SecurityID
		and t1.Year			=	t2.Year
		and t1.Month		= 	t2.Month
		and t1.CallPut		<	t2.CallPut
	order by SecurityID, Year, Month
;quit;

*Delta variables;
data Data_OP1 (DROP = Month_gap);	set Data_OP1;
	by SecurityID Year Month;
	
	Month_gap 		= month - lag(month);
	P_Volume_delta	= P_Volume 	- lag(P_Volume);
	P_OI_delta		= P_OI 		- lag(P_OI);
	C_Volume_delta	= C_Volume 	- lag(C_Volume);
	C_OI_delta		= C_OI 		- lag(C_OI);

	if first.SecurityID or Month_gap not in (1,-11)	then do;
		P_Volume_delta	=	.;	
		P_OI_delta		=	.;
		C_Volume_delta	=	.;
		C_OI_delta		=	.;
	end;
run;

*Ratio variables;
data Data_OP1;	set Data_OP1;
	format CP_Volume 7.2 CP_OI 7.2;
	if P_Volume >0 	then CP_Volume = C_Volume/P_Volume;
				   	else CP_Volume = .;

	if P_OI >0 		then CP_OI	= 	C_OI/P_OI;
					else CP_OI	=	.;	
run;


/*5.3 Add variables from DATA_OP to DATA_VS								*/
proc sql;
	create table DATA_VS1 as 
	select t1.*, t2.C_Volume, t2.C_OI, t2.P_Volume, t2.P_OI,
				 t2.CP_Volume, t2.CP_OI,
				 t2.P_Volume_delta, t2.P_OI_delta, t2.C_Volume_delta, t2.C_OI_delta	
	from DATA_VS as t1
	left join DATA_OP1 as t2
		on 	t1.SecurityId 	= t2.SecurityID
		and t1.Year 		= t2.Year
		and t1.Month 		= t2.Month
	order by SecurityID, Year, Month
;quit;
*Note: There are cases (e.g. SecurityID = 155081) where ImpVol exists but 
 	   trading volume does not. OptionMetrics clarified that ImpVol requires
	   sufficient data/price points, and not necessarily any trading;

*Arrange columns - Bring Year and Month columns forward;
data Data_VS1; 
	Retain SecurityID Date Year Month;
	set DATA_VS1;
run;

*Update;
data Data_VS;	set Data_VS1;		run;
data Data_OP;	set Data_OP1;		run;


/*5.4 Save datasets to local folder											*/
data DBfolder.Data_VS;					set Data_VS; 			run;
data DBfolder.Data_OP;					set Data_OP; 			run;


*Delete temporary datasets;
proc datasets noprint; delete List_filenames Data_VS1 Data_OP1; run;
**********************************END***************************************;













******************************************************************************
PURPOSE		: 	(1) Merges the raw OptionMetrics monthly data with Header 
				    Information	provided in other OptionMetrics data files.
				(2) Merges CRSP/Compustat linking table and CRSP attributes such 
				    as	Share Code and Exchange Code.
INPUT		: 	DATA_VS (created from monthly OptionMetrics datafiles)
OUTPUT		: 	DATA_VS_MERGED
COMMENTS	:	Idea is to match OptionMetrics data with CRSP/Compustat using 
				CUSIPs
AUTHOR		:	Udit Gupta (September, 2015)
*****************************************************************************

*****************************************************************************
PRELIMINARIES
****************************************************************************;
* Setting up ODBC connection, and loading the Schema;
libname IVyDB odbc complete='DSN=SQLServerProdN;Database=IVyDB' schema=dbo;

* Location of Datasets;
libname DBfolder '\\dev-sasbi101\temp\Gupta\Datasets';

* Run Get_Linktable program - to get Linktable & CRSP_Meta datafiles;
%include '\\dev-sasbi101\temp\Gupta\Code\Get_LinkingTable_J23.sas';


****************************************************************************
STEP 1: Load OptionMetrics Dataset into WorkDB
****************************************************************************;
data WorkDB; 
	set DBFolder.Data_VS;
run;

****************************************************************************
STEP 2: 
OBJECTIVE: Merge data from OptionMetrics with CRSP
- We achieve this by creating a link betweem SecurityID and PERMNO using CUSIPs
  [SecurityID -> CUSIP -> PERMNO]
- [SecurityID + CUSIP] should be 100% match between VOLATILITY_SURFACE
  and SECURITY_NAME data files. 
- [CUSIP + PERMNO] is taken from the CRSP-COMPUSTAT LINKING TABLE. 
  We get a 96% match. 

PROCESS:
2.1: Merge VOLATILITY_SURFACE and SECURITY_NAME (adds CUSIPs)
2.2: Merge SECURITY datafile (adds Flags)
2.3: Merge LINKTABLE (adds PERMNO for primary securities)
2.4: Save cleaned dataset	
****************************************************************************;

/*2.1: Merge VOLATILITY_SURFACE and SECURITY_NAME (adds CUSIPs)
Comment				:	100% MATCH at this level (after 2 merges) 	
Primary Key Used	:	SecurityID + DateTime Value
New Columns Added	:	CUSIP, IssuerDescription (Co. name), 
				   		IssueDescription, Class, Period (From & To)
Output Dataset		:	Temp_Merge1											*/

*Prepare SECURITY_NAME data file;
data Security_name;
	set IvyDB.Security_name;
	date = datepart(date);
	format date MMDDYY8.;
run;

*Using date variables, create FROM and TO variables;
proc sort data =  Security_name; by SecurityID descending date; run;
data Security_name;	set Security_name;
	by SecurityID;
	FROM = date;	
	TO   = lag(date)-1;		

	format FROM MMDDYY8. TO   MMDDYY8.;
	if first.SecurityID then TO = mdy(2,2,2222); 
	*Date are in descending order. Set TO to dummy future date;
run;
proc sort data =  Security_name; by SecurityID date; run;

*Merge WORKDB with SECURITY_NAME dataset;
proc sql;
	create table temp_Merge1 as
	select 	t1.*, 
		 	t2.CUSIP, t2.Ticker, t2.IssuerDescription, 
			t2.IssueDescription, t2.Class
	from WorkDB as t1
		left join Security_name as t2
		on t1.SecurityID = t2.SecurityID
			and t1.Date >= t2.FROM 
			and t1.Date <= t2.TO		
;
	title '2.1 Certain Obs are not matched to CUSIPs due to deficiency in Security_Name file';
	select nmiss(CUSIP) as CUSIP_missing, count(*) as TotalObs
	from temp_Merge1
;
quit;

*Subset matched obs;
data temp_merge1_matched;
	set temp_merge1;
	if ~missing(CUSIP);
run;

*___________________________________________________________________________
 RESOLVING FOR UNMATCHED VALUES
 ISSUE:
 For over 100 securities, the historical record (in the SECURITY_NAME 
 data file) does not start as early as the actual data in the 
 VOLATILITY_SURFACE file. This creates missing values when trying to 
 match the VOLATILITY_SURFACE data with matching CUSIPs from that time 
 period. E.g, SecurityID 7365 has data in VOLATILITY_SURFACE file 
 starting Jan ’96, however, its earliest record in SECURITY_NAME file 
 starts from April 2000.
 
 COMMENT FROM OPTIONMETRICS:
 There are some inconsistencies between SECURITY_NAME and 
 VOLATILITY_SURFACE tables. However, addressing those inconsistencies 
 is not a high priority to us at this time 
 
 RESOLUTION:
 Code below changes the date of the first entry for the effected
 securities in the SECURITY_NAME table, by moving the FROM date back
 in time to the start of the OM dataset (1 Jan 1996)
___________________________________________________________________________;

*Unmatched rows - 1508 rows with missing CUSIPs;
data temp_merge1_unmatched;
	set temp_merge1;
	if missing(CUSIP);
	Drop CUSIP TICKER IssuerDescription IssueDescription CLASS;
run;

*Create a MODIFIED SECURITY_NAME table to adjust for discrepancy;
Proc sql;
	*~106 SecurityIDs don't find a match;
	create table temp_names as
	select distinct SecurityID
	from temp_merge1_unmatched
;
	*Create a subset of SECURITY_NAME file for those 106 SecurityIDs;
	create table Security_name_subset as
	select SN.*
	from Security_name as SN
	inner join temp_names as T
		on SN.SecurityID = T.SecurityID
	order by SecurityID, Date
;
quit;

*Move the start data back to database start date;
data Security_name_subset;	set Security_name_subset;
	by SecurityID Date;
	if first.SecurityID then FROM = mdy(12,31,1995);
			*Because OptionMetrics data starts from 1 Jan 1996;
run;

*Fixing the issue - by merging the unmatched values with the 
 modified Security_Name table;
proc sql;
	create table temp_Merge1_fixed as
	select 	t1.*, 
			t2.CUSIP, t2.Ticker, t2.IssuerDescription,  
			t2.IssueDescription, t2.Class
	from temp_merge1_unmatched as t1
		left join Security_name_subset as t2
		on t1.SecurityID = t2.SecurityID
			and t1.Date >= t2.FROM
			and t1.Date <= t2.TO	
	order by SecurityID, Date;
quit;

*There should not be any missing values now;
proc sql;
	title '... After fixing for missing CUSIPs (if still missing, read note)';
	select nmiss(CUSIP) as CUSIP_missing, count(*) as TotalObs
	from temp_Merge1_fixed
;quit;

data temp_Merge1_fixed;	set temp_Merge1_fixed;
	if missing(CUSIP) =0;
run;
	*SecurityID: 3 obs dropped for 11769 and 1128515. This has been fixed 
	 in OptionMetrics backend. However, they will continue to appear till 
 	 next update release, scheduled for October 2015;


*Merge the two pieces together;
data temp_merge1;
	set temp_merge1_matched		temp_Merge1_fixed;
run;
proc sort data=Temp_Merge1; by SecurityID Date; run;

proc sql;
	title '...No missing CUSIPs going forward to next step';
	select nmiss(CUSIP) as CUSIP_missing
	from Temp_merge1;
quit;

*Delete intermediate datasets;
proc datasets noprint; delete 	temp_merge1_matched	temp_merge1_unmatched
								temp_Merge1_fixed	Security_name_subset
								Security_name	temp_names;				
run;

/*2.2: Merge SECURITY datafile (adds Flags)
Comment				:	100% MATCH at this level
						Because most of the attributes added here are not 
						completely accurate, we dont actually use them at 
						any point going forward. 
						However, as the quality of these attributes improve 
						in OM's datafiles in future, they may be useful.
Primary Key Used	:	SecurityID
New Columns Added	:	IssueType, IndexFlag,  ExchangeFlags
Output Dataset		:	Temp_Merge2											*/

proc sql;
	create table temp_Merge2 as
	select 	t1.*, 
			t2.SecurityID as flag, 
           	t2.IssueType, t2.IndexFlag, t2.ExchangeFlags 
	from temp_merge1 as t1
		left join IvyDb.Security as t2
		on t1.SecurityID = t2.SecurityID
	order by SecurityID, Date;
quit;

*The above step has 100% match during merge, as expected;
proc sql;
	title '2.2: Merge with SECURITY datafile';
	select count(*) as count, nmiss(flag) as missing, 1-nmiss(flag)/count(*) as MatchRatio
	from temp_Merge2;
quit;

data temp_Merge2; set temp_Merge2 (drop= Flag); run;


/*2.3: Merge LINKTABLE
Comment				:	Execute code from another file to generate 
						LINKTABLE. 96% MATCH at this level. Please refer
						to Get_Linktable file for more details on Linktable.	
Primary Key Used	:	CUSIP + YEAR + MONTH
New Columns Added	:	PERMNO, LinkPrim, WS_ID, GVKEY, Comnam, 
						Ticker, ShrCd, EXCHCD
Output Dataset		:	Temp_Merge3											*/

proc sql;
	*LEFT JOIN;
	create table temp_Merge3 as
	select 	t1.*,
		   	t2.PERMNO, t2.PERMCO, t2.LinkPrim, t2.WS_ID, t2.GVKEY,
			t2.Comnam, t2.Ticker as CRSP_Ticker, t2.ShrCd, t2.EXCHCD
	from temp_Merge2 as t1
	left join Linktable as t2
		on 	t1.CUSIP = t2.CRSP_CUSIP
		and	t1.YEAR  = t2.YEAR
		and t1.MONTH = t2.MONTH
	order by SecurityID, Date;
		*Comment: Linktable also has attributes such as 'LinkType', 'IID',
		 'LinkID', 'UsedFlag', which haven't been added here;


	*Match ratio - 96%;
	title '2.3: Merge Linktable attributes';
	select count(CASE WHEN PERMNO is not NULL THEN 1 END) as Matched,
		   count(CASE WHEN PERMNO is     NULL THEN 1 END) as Unmatched,
		   count(CASE WHEN PERMNO is not NULL THEN 1 END)/count(*) as MatchRatio
	from temp_Merge3;

*Comment: 
 - Unmatched (26,306) values imply OM obs which don't have a match in LinkTable
   (LinkTable is filtered to retain cases where LinkPrim = 'P' or 'C').
 - 2,497/26,306 (~10%) of the unmatched values here, DO find a match in CRSP_Meta
   dataset (created from CRSP monthly datafile).
 - 90% (23,809) neither have a match in CRSP monthly file or primary linking table.
 - Of these 23,809, most fall into one of the following categories:
	 - are an Index (i.e. ExchangeFlags = 32768, or IndexFlag = '1'
	 - Issue Type is not ordinary share (i.e. IssueType = 'A', '7','F','%')
	 - Implied Vol is missing (there are cases when OptionMetrics continues to give
	   missing values, few months after delisting as well).
	 - 2,108/23809 obs do not fall into any of the above categories. But this includes 
	   incorrectly catagorized cases by OptionMetrics, because on visual inspection, 
	   one can see cases of type Index/ ADR etc. Since this is a small set, we do not 
	   investigate further;
quit;


/*2.4: Save merged dataset													*/

*Arrange columns for easy view (First identifiers then variables);
data Temp_Merge3;
	Retain Permno SecurityID CUSIP Date Year Month Comnam CRSP_Ticker WS_ID Permco GVKEY 
		   LinkPrim ShrCd EXCHCD IssueType IndexFlag ExchangeFlags 
		   IssuerDescription Ticker IssueDescription Class;
	set Temp_Merge3;
run;

*Saving Dataset to a local folder;
data DBfolder.Data_VS_Merged;	set Temp_Merge3; 	run;

*Delete intermediata datasets;
proc datasets noprint; delete WorkDB Temp: Linktable CRSP_Meta ; run;

**********************************END***************************************;







**codes below were for running certain tests********************************;


/*
*EXTRA - Distribution of Cleaned data;
proc sql;
	title 'Distribution of Data by Share Code and Exchange Code';
	select SHRCD, EXCHCD, count(*)
	from WorkDB
	group by SHRCD, EXCHCD;
quit;
*/

/*
*EXTRA - All options have American exercise type;
proc sql;
	create table work_securityID as
	select distinct SecurityId
	from WorkDB;

	create table work_securityID_check as
	select  t1.*, t2.ExerciseStyle
	from work_securityID as t1
		left join IvyDb.Option_Info as t2
		on t1.SecurityID = t2.SecurityID;

	select ExerciseStyle, count(*) as ct
	from work_securityID_check
	group by ExerciseStyle;
quit;

*Delete intermediata datasets;
proc datasets noprint; delete work_securityID work_securityID_check;
run;	

*EXTRA - IssueType is not properly defined in OptionMetrics;
proc sql;
	title 'IssueType other than 0 or BLANK are incorrectly marked in OptionMetrics';
	select SHRCD, ISSUETYPE, count(distinct LPERMCO) as ct
	from WorkDB
	group by  SHRCD, ISSUETYPE ;
quit;														*/
	






/* NOTE 1:
*We filter using CRSP SHARE CODE, since ISSUETYPE FLAG IS BROKEN!!!;
proc sql;
	select ISSUETYPE, CRSP_SHRCD, count(distinct SECURITYID) as ct
	from temp_Merge3_matched
	group by ISSUETYPE,CRSP_SHRCD ;
quit;

*E.g. Incorrectly marked ADRs;
proc sql;
	create table test as
	select distinct SecurityID
	from temp_Merge3_matched
	where ISSUETYPE = '0' and CRSP_SHRCD = 31
	order by SecurityID, date;
quit;

proc sql;							*(refer to the excel);
create table test1 as
select *
from Ivydb.security
where SECURITYID in (102276, 112253, 122338, 147684, 147804, 
					 148059, 148643, 162295, 189594, 189669);

create table test2 as
select *
from Ivydb.security_name
where SECURITYID in (102276, 112253, 122338, 147684, 147804, 
					 148059, 148643, 162295, 189594, 189669);
run;





/*
Issues with OptionMetrics Flags:

>>We filter out all IssueTypes except '0' & 'blank'
*Clarification from OptionMetrics:
	- <Blank> values for IssueType are still being categorized. 
	  > Hence it is better not to filter those out;
*Almost all 'relevant' entries are filtered using IssueType. 	
	0 – Common Stock	(blank) – Unspecified
	A – Market index	7 – Mutual or investment trust fund
	F – ADR/ADS			% – Exchange-traded fund;

>>Filter out ExchangeFlags = 32768
*Clarifications from OptionMetrics:
	- there can be one-off cases like that of SID 104189,
      where ExchangeFlag indicated it was an Index, but IndexFlag was 0. 
 	  > To avoid such contradictions in data, better to have additional 
	    checks.
>>Do Not Filter out ExchangeFlags = 16
	- While ExchangeFlag = 16 indicates that the security is on OTC.
	  This attribute is not updated. Some cases with value of '16' 
	  were noticed to be trading on NYSE/NASDAQ. OptionMetrics is 
	  carrying out the necessary quality check on this.
	  > Hence we do not filter out securities with ExchangeFlags = 16;

>>Filter out IndexFlag = '1'
*Redundant given first filter, added for abundant caution;
*/



/*CHECKS carried out on raw datasets;

*Prepares datasets for the checks carried out below;
data workdb;
	set Data_VS;
	year  = year(date) ;
	month = month(date);
	day   = day(date);
run;

data Security;
	set IvyDB.Security;
	CUSIP  = substr(CUSIP,1,8);
	CUSIP6 = substr(CUSIP,1,6);
run;


*****ISSUE: 1 Company with multiple SecurityIDs**************;
* First 6 digits of CUSIP uniquely identifies an Issuer.
  Using this fact, we run some basic checks to identify 
  instances where 1 stock has multiple SecurityIDs;
* Lists out all instances of multiple SecurityIDs for 1 Stock
  e.g. Discovery Communication - DISCA, DISCB and DISCK;
proc sql;
	create table temp_security as
	select Security.*
	from Security
	inner join 
		(	select CUSIP6, count (*) as ct
			from Security
			group by CUSIP6
			having ct >1 ) as T
		on Security.CUSIP6 = T.CUSIP6

	where IssueType = '0' and IndexFlag = '0'
	order by Cusip;
quit;	*766 rows;

* While SECURITY file may show that 1 Company has multiple class
  of shares, it doesn't imply that all those class of shares will 
  have options trading on them;
data temp;
	set Data_VS;
	where SecurityID in (124221, 124166, 137684);
	*No data shows for 124221 - Class B shares of Discovrey Comm.
	 This is because no options are traded on them. However, the
	 SECURITY file (query above) still contains its reference;
run;
*****************************************************************;


*****ISSUE: 3 Instances of missing SecurityID information********;
* Check if all entries in DB will have corresponding details 
in Security file - ideally the following should results in 0 obs.;
proc sql;
	select distinct W.SecurityID
	from WorkDB as W
	left outer join Security as S
		on W.SecurityID = S.SecurityID
	where S.CUSIP is NULL;	*this line gives the missing values;
quit;

*UPDATE FROM OPTIONMETRICS - '104327' was a mistake which they have 
corrected now in their production system. Other 2 SecurityIDs have 
been removed as part of the clean-up effort.

*EARLIER - we'd get 3 Security IDs with missing CUSIPs. 2 of these 
 didnt have an entry in Security files at all;
data temp;
	set IvyDB.Security_name;
	where securityID in (104327, 11769, 1128515);
run;

data temp;
	set IvyDB.Security;
	where securityID in (104327, 11769, 1128515);
run;

*WorkDB contains observations for all 3. However for 2 of these, 
 all attributes are set at -99.99;
data temp;
	set WorkDB;
	where securityID in (104327, 11769, 1128515);
run;
*****************************************************************;


*****Why use SECURITY_NAME and not SECURITY data file ***********;
*Security_Name file contains a historical record of changes 
 to the ticker, issuer and issue descriptions, and CUSIP’s 
 for a security. For our purpose we are concerned about changes to 
 CUSIPs, hence we will investigate those cases;
*Security file only contains the most recent entry, hence will not
 provide information about CUSIP changes, as presented below;

*Instances of updates to the SecurityID information;
proc sql;		
create table temp_securitychange as
	select SN.*
	from IvyDB.Security_name as SN
	inner join 
	(	select SecurityID, count(distinct CUSIP) as ct
		from IvyDB.Security_name
		group by SecurityID
		having ct >1) as T
	on SN.SecurityID = T.SecurityID;
quit;

*~12,000 SecurityIDs have changes in information. Some of them are
CUSIP related, others are not;
proc sql;		
	select count( distinct SecurityID)
	from temp_securitychange;
quit;
*****************************************************************/



/*
*Same PERMCO with multiple primary securities;
proc sql;
	create table test as
	select *, count(distinct SecurityID) as ct
	from temp_Merge3_clean
	where ImpliedVolatility>0
	group by Year, Month, LPERMCO
	having ct >1
	order by LPERMCO;	*SecurityID;
quit;

proc sql;
	select YEAR, count(*)/4
	from test
	group by YEAR;
quit;

proc sql;
	title 'Example from Linktable (raw) and Linktable (adjusted)';
	create table t1 as
	select *
	from DBfolder.crsp_compu_linkingtable
	where LPERMNO in (26403, 87436);

	select *
	from linktable_matched
	where LPERMNO in (26403, 87436);

	create table temp as
	select *
	from CRSP_data
	where PERMCO = 20587;
quit;

*/

**********************************END***************************************;
