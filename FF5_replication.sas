/***********************************************************************************
Program:
1) Replicating the 5 Fama-French Factors.
   Based on "A Five-Factor Asset Pricing Model" by Fama & French (2014)
   Link to the paper is available on French's website, along with 
   monthly return series

2) Replicating the Momentum Factor.
   Based on "On Persistence in Mutual Fund Performance" by Carhart (1997)
   The monthly return series is available on French's website.

By: Udit Gupta
***********************************************************************************/

libname rawdata "\\dev-sasbi101\temp\Gupta\Datasets";

%let output			= \\dev-sasbi101\temp\Gupta\Output;
%let raw_Compustat 	= compustat_201412;		*CRSP/COMPUSTAT Merged;	 
%let raw_CRSP 		= crsp_201412;			*CRSP;
%let yrStart 		= 1960;					*Include buffer to the Time Series start year;
%let yrEnd 			= 2016;					*Include buffer to the Time Series end year;
%let yrTSStart 		= 1963;					*Return time series start year;
%let yrTSEnd		= 2015;					*Return time series end year;
%let mnthTSStart	= 7;					*Return time series start month;
%let mnthTSEnd		= 4;					*Return time series end month;


/*******************************************************************************
STEP 1: Read and clean the data (time period is restricted to the paper)
(1.1) Fundamentals data from COMPUSTAT (source: CRSP/COMPUSTAT merged)
(1.2) Returns data from CRSP
	  - Clean for 1-to-Many pairs of PERMCO & PERMNO. 
		Add all MCap and assign to biggest MCap PERMNO.
*******************************************************************************/

/*(1.1) Fundamentals data from COMPUSTAT*/
data Fundamentals;
	set rawdata.&raw_Compustat(rename=(gvkey=gvkey_char)); 
			cusip	=	substr(cusip,1,8);		*raw data has 9 digits, 8 = 6(Company) + 2(Security);
			gvkey	=	gvkey_char*1;			*From GVKEY(TEXT) to GVKEY(NUMERIC);

			year 	= 	year(datadate);			*new variable - based on data release data;
			month	= 	month(datadate);		*new variable - based on data release data;
			
	rename 	LPERMNO = 	PERMNO;					*CRSP calls it PERMNO;
	rename 	LPERMCO = 	PERMCO;					*CRSP calls it PERMNO;

	if (datadate > mdy(1,1,&yrStart)) and (datadate < mdy(1,1,&yrTSEnd)) 
									  and (LINKPRIM in ('P','C'));		*Primary identified by Crsp or Compustat;

	*Calculating Book Value of Equity variable;
	if missing(TXDITC) then TXDITC =0;
			PS		= coalesce(PSTKRV, PSTKL, PSTK,0);
			SE 		= coalesce(SEQ, sum(CEQ,PS), AT - LT);
			BE	 	= SE - PS + TXDITC;
	label 	BE 		= 'Book value of equity';

	Keep 	GVKEY LPERMCO LPERMNO CONM TIC CUSIP EXCHG DATADATE YEAR MONTH FYEAR PDDUR 
		 	AT BE COGS REVT XINT XSGA;				*Financial variables;
run;

* Arrange columns;
data Fundamentals;
	Retain GVKEY PERMCO PERMNO CONM TIC CUSIP EXCHG DATADATE YEAR MONTH FYEAR PDDUR 
		   AT BE COGS REVT XINT XSGA;				*Financial variables;
   	set Fundamentals;
run;

* Check for duplicates;
* Linkprim used in step one ensures there are no duplicate keys as follows;
proc sort data = Fundamentals nodupkey; by GVKEY DATADATE; run;

/*(1.2) Returns data from CRSP */
* We read returns data in Three mutually exclusive datasets 
  1) With    DLRET (with or without RET) - when a stock delists
  2) Without DLRET (with            RET) - usual case
  3) Without DLRET (        without RET) - when a stock lists;

data Ret_delist;
	set rawdata.&raw_CRSP;
	
	where (missing(DLRET) = 0) and
	      (date > mdy(&mnthTSStart-1,1,&yrTSStart-1) ) and 
	      (date < mdy(&mnthTSEnd+1,1,&yrTSEnd  ) ) ;

			prc 	= 	abs(prc); 					*Price is negative when calculated from bid/ask average;
			mcap 	= 	abs(prc * SHROUT);			*abs used because price is negative when calculated from bid/ask;
	label 	mcap 	= 	"Market Capitalization";

			year	=	year(DATE);				
			month	=	month(DATE);

			ret  = sum(1,ret)*sum(1,dlret)-1;
			retx = sum(1,retx)*sum(1,dlretx)-1;
			
	keep 	PERMCO PERMNO YEAR MONTH SHRCD EXCHCD 
			RET RETX MCAP PRC;
run;

data Ret_normal;
	set rawdata.&raw_CRSP;
	
	where (missing(RET) = 0) and (RET >= -1) and (missing(DLRET))  and
	      (date > mdy(&mnthTSStart,1,&yrStart+1)) and 
	      (date < mdy(&mnthTSEnd+1,1,&yrTSEnd)) ;

			prc 	= 	abs(prc); 					*Price is negative when calculated from bid/ask average;
			mcap 	= 	abs(prc * SHROUT);			*abs used because price is negative when calculated from bid/ask;
	label 	mcap 	= 	"Market Capitalization";

			year	=	year(DATE);				
			month	=	month(DATE);

	keep 	PERMCO PERMNO YEAR MONTH SHRCD EXCHCD 
			RET RETX MCAP PRC;
run;

data Ret_list;
	set rawdata.&raw_CRSP;
	
	where (missing(DLRET)) and (missing(RET))     and
	      (date > mdy(&mnthTSStart,1,&yrStart+1)) and 
	      (date < mdy(&mnthTSEnd+1,1,&yrTSEnd)) ;

			prc 	= 	abs(prc); 					*Price is negative when calculated from bid/ask average;
			mcap 	= 	abs(prc * SHROUT);			*abs used because price is negative when calculated from bid/ask;
	label 	mcap 	= 	"Market Capitalization";

			year	=	year(DATE);				
			month	=	month(DATE);

			ret  	= .;
			retx 	= .;
			
	keep 	PERMCO PERMNO YEAR MONTH SHRCD EXCHCD 
			RET RETX MCAP PRC;
run;

data Returns;
	set Ret_normal Ret_delist Ret_list;
run;

* Remove rows with no data - such as when a stock remains listed but is not trading
  e.g. PERMNO 22584;
data Returns;
	set Returns;
	if missing(MCap) and missing(RET) then delete;
run;

*Arrange columns;
data Returns;
	retain	PERMCO PERMNO SHRCD EXCHCD YEAR MONTH PRC RET RETX MCAP;
	set Returns;
run;

proc datasets noprint; delete Ret_normal Ret_delist Ret_list; run;

* ISSUE: When (1) PERMCO has (Many) PERMNO for given 'year month';
/*proc sql;*/
/*	create table temp as */
/*	select T.**/
/*	from (*/
/*			select *, count(*) as count*/
/*			from Returns*/
/*			group by YEAR, MONTH, PERMCO*/
/*		 ) as T*/
/*	where T.count > 1*/
/*	order by count desc;*/
/*quit;*/

* CORRECTION;
proc sort data = returns; by YEAR MONTH PERMCO MCAP; run; *MCAP is ascending order;
data returns;
	set returns;
	by YEAR MONTH PERMCO MCAP;
	retain MCapAdj;

	if first.permco and last.permco then do;
		McapAdj = Mcap;
		output;
	end;
	else do;
		if first.permco then McapAdj = MCap;
						else MCapAdj = sum(MCapAdj,Mcap);
		if last.permco then output;						*Give added MCap to the Biggest stock!;
	end;
run;


/************************************************************************************
STEP 2: Prepare data for Fama-French Portfolios
(2.1) Create 'Portfolio_YEAR' and 'Portfolio_MONTH' variables
      Filter Returns dataset for Share Code = 10/11 ('Simple' Ordinary Common Shares)
(2.2) Clean Accounting data in Fundamentals dataset
(2.3) Create Proxy variable - Operating Profitability (scaled by Book Equity)
(2.4) Create Proxy variable - Change in Total Assets (scaled by Total Assets)
(2.5) Add MCap for (a) December (year t-1) and (b) June (year t)
(2.6) Create Proxy variable - Book/MarketValue
*************************************************************************************/

/*(2.1) Filter CRSP data for Share Code = 10/11 ('Simple' Ordinary Common Shares)				*/
data Returns;
	set Returns;
	where SHRCD in (10, 11);
run;

*Adding new columns to Returns dataset - 'Portfolio Year' and 'Portfolio Month';
data ReturnsAdj;
	set Returns;
	if month <= 6 then do;
		Portfolio_Year	  = YEAR  - 1;
		Portfolio_Month   = MONTH + 6;
	end;
	else do;
		Portfolio_Year	  = YEAR;
		Portfolio_Month   = MONTH - 6;
	end;
run;

data Fundamentals;
	set Fundamentals;

	Portfolio_year = YEAR + 1; *Fiscal data released in year N, is available for Portfolio in June of year N+1;
run;

/*(2.2) Clean accounting variables in Fundamentals dataset										*/
* Filtering criterion as per Fama & French's paper.
  SMB, HML, RMW, and CMA for July of year t to June of t+1 include all NYSE, AMEX, and NASDAQ stocks for which we have 
  (1) market equity data for December of t-1 and June of t, 
  (2) (positive) book equity data for t-1 (for SMB, HML, and RMW)
  (3) non-missing revenues and at least one of the following: 
		cost of goods sold, 
		selling, general and administrative expenses, or 
		interest expense for t-1 (for SMB and RMW), 
  (4) total assets data for t-2 and t-1 (for SMB and CMA);

data Fundamentals;
	set Fundamentals;
	
	*Balance Sheet items;
	if 	(AT  <= 0)  then AT   = .;		*Negative Total Assets wont make sense - skip that data;
	if  (BE  <= 0)	then BE   = .;		*Treat negative book value as missing;
										*Use missing values as a flag to skip rows;
	*P&L items;
	if (missing(COGS) = 0) or (missing(XINT) = 0) or (missing(XSGA) = 0) 
	then do;
		if missing(COGS) 			then COGS = 0;
		if missing(XINT) 			then XINT = 0;
		if missing(XSGA)			then XSGA = 0;
	end;
	else REVT = .;	* If none of COGS/XINT/XSGA exists, we want to skip that row;
					* Use missing(REVT) as a flag to skip rows;
run;

/*(2.3) Create Proxy variable - Operating Profitability (scaled by Book Equity)					*/
data Fundamentals;
	set Fundamentals;	

	if missing(BE) or missing(REVT)
		then ff_profitability	=	. ;					
	else     ff_profitability	= 	(REVT - COGS - XINT - XSGA)/BE ; *Defined in the paper;
run;


/*(2.4) Create Proxy variable - Change in Total Assets (scaled by total assets)					*/
*Add dummy 'line' variable - assists with creating lag variables;
proc sort data = Fundamentals; by GVKEY YEAR FYEAR; run;
data Fundamentals;
	set Fundamentals;
	by GVKEY YEAR FYEAR;

	AT_1 	= 	lag(AT); 		*Total Assets lagged by 1 year;

	retain line_gvkey;
	if first.GVKEY 
		then line_gvkey = 1;	*We do NOT want to use condition as first.PERMNO here;
	else     line_gvkey = line_gvkey + 1;
run;

*Create the proxy variable;
data Fundamentals;
	set Fundamentals;	

	if line_gvkey = 1 then do;
				 AT_1   = . ;			*Lagged value is from another GVKEY, hence incorrect;
				 ff_inv	= . ;	
	end;

	else do;
		if missing(AT) or missing(AT_1)
			then ff_inv	=	. ;								
		else     ff_inv = 	(AT - AT_1)/(AT_1);
	end;
run;

/*(2.5) Add MCap for (a) December (year t-1) and (b) June (year t)								*/
* Mcap from June of 't' year;
proc sql;
	create table MCap_June as 
	select PERMNO, 
		   YEAR   			as Portfolio_year, 
		   MONTH , 
		   MCAPAdj  			as MCap_June, 
		   sum(MCap_June) 	as MCap_June_sum,
		   EXCHCD
	from Returns
	where MONTH = 6
	group by Portfolio_year
	order by PERMNO, Portfolio_Year;
quit;

* Merging with Fundamentals dataset - June MCAP; 
proc sort data = Fundamentals; by PERMNO Portfolio_year; run;
proc sort data = MCap_June;    by PERMNO Portfolio_year; run;

data Fundamentals;
	merge Fundamentals (in = k) MCap_June (drop = MONTH) ;
	by PERMNO Portfolio_year;
	if k;						
run;

* Mcap from December of 't-1' year;
data MCap_PY  (Keep = PERMNO YEAR MONTH MCAP_PY);
	set Returns;
	if month = 12 then Output;
	rename 	MCAPAdj  = MCAP_PY;
run;

*Merging with Fundamentals dataset - December MCAP; 
proc sort data = Fundamentals;  by PERMNO YEAR; run;
proc sort data = MCap_PY;       by PERMNO YEAR; run;

data Fundamentals;
	merge Fundamentals (in = k) MCap_PY (drop = MONTH) ;
	by PERMNO YEAR;
	if k;						
run;

/*(2.6) Create Proxy variable - Book/MarketValue												*/
data Fundamentals;
	set Fundamentals;
	if missing(MCAP_PY) or missing(BE)
		then ff_BM =.;
	else 	 ff_BM = BE*1000/MCAP_PY;
run;

/********************************************************************************
STEP 3: Create Sub-portfolio
(3.1) Create Breakpoints on 4 variables - Size, B/M, OP and INV (INDEPENDENT SORTS)
(3.2) Create Portfolio Master - Data for NYSE, ASE, NASDAQ stocks for all years
(3.3) Merge Breakpoint dataset with Portfolio Master
(3.4) Merge CRSP with Portfolio Master 
(3.5) Subset to create Fama French Portfolios
(3.6) Create return time series for all the above portfolio (using a MACRO)
(3.7) Create return time series for Factor
*********************************************************************************				*/

/*(3.1) Create Breakpoints and Rank stocks on 4 variables (INDEPENDENT SORTS)					*/

*50 Percentile (Median) breakpoint from NYSE listed stocks;
data data_breakpoint_MCap;
	set Mcap_June;
	where EXCHCD in (1,31) and (missing(MCap_June) = 0);		*1 stands for NYSE in CRSP data;
run;

proc sort 		data = data_breakpoint_MCap; by Portfolio_Year MCap_June; run;
proc univariate data = data_breakpoint_MCap noprint;
	var MCap_June ;
	by Portfolio_Year;
	output out = Stat_Mcap_June
	pctlpts = 50 pctlpre = MCAP;
run; 

*30/70 Percentile breakpoints for BM;
data data_breakpoint_BM;
	set Fundamentals;
	where (EXCHCD in (1,31)) and 
		  (missing(Mcap_PY) = 0) and (BE > 0);* or EXCHG in (11);
run;

proc sort data  = data_breakpoint_BM; by Portfolio_Year ff_BM; run;
proc univariate data = data_breakpoint_BM noprint;
	var ff_BM ;
	by Portfolio_Year;
	output out = Stat_BM_percentiles
	pctlpts = 30 70 pctlpre = BM;
run; 

*30/70 Percentile breakpoints for OP;
data data_breakpoint_OP;
	set Fundamentals;
	where (EXCHCD in (1,31) ) and 
	      (BE > 0) and (missing(ff_Profitability) = 0);*or EXCHG in (11);
run;

proc sort data  = data_breakpoint_OP; by Portfolio_Year ff_Profitability; run;
proc univariate data = data_breakpoint_OP noprint;
	var ff_Profitability ;
	by Portfolio_Year;
	output out = Stat_OP_percentiles
	pctlpts = 30 70 pctlpre = OP;
run; 

*30/70 Percentile breakpoints for Inv;
data data_breakpoint_Inv;
	set Fundamentals;
	where (EXCHCD in (1,31) ) and 
		  (missing(ff_inv) = 0);*or EXCHG in (11);
run;

proc sort data  = data_breakpoint_Inv; by Portfolio_Year ff_inv; run;
proc univariate data = data_breakpoint_Inv noprint;
	var ff_inv ;
	by Portfolio_Year;
	output out = Stat_Inv_percentiles
	pctlpts = 30 70 pctlpre = Inv;
run; 

*Number of Firms used in calculating Break-points;
proc sql;
	create table Breakpoint_firms as
	select 	ME.Portfolio_year, 
		   	ME.Count as MEcount, 
			BM.Count as BMcount,
			INV.Count as INVcount,
			OP.Count as OPcount

	from
		(select 	Portfolio_year, count(*) as Count 
		 from data_breakpoint_mcap
		 group by Portfolio_year)  as ME 

	inner join
		(select 	Portfolio_year, count(*) as Count 
		 from data_breakpoint_bm
		 group by Portfolio_year)  as BM
	on  ME.Portfolio_Year = BM.Portfolio_Year

	inner join
		(select 	Portfolio_year, count(*) as Count 
		 from data_breakpoint_inv
		 group by Portfolio_year)  as INV
	on ME.Portfolio_Year = INV.Portfolio_Year

	inner join
		(select 	Portfolio_year, count(*) as Count 
		 from data_breakpoint_op
		 group by Portfolio_year)  as OP
	on ME.Portfolio_Year = OP.Portfolio_Year;
quit;


*Merge break-points into Summary dataset;
/*proc sort data = PortfolioMaster; by Portfolio_Year PERMNO; run;*/
data Summary_Breakpoints;
	merge Stat_Mcap_June 
		  Stat_BM_percentiles 
		  Stat_OP_percentiles 
		  Stat_Inv_percentiles
	 	  Breakpoint_firms;
	by Portfolio_year;
run;

data Summary_Breakpoints;
	set Summary_Breakpoints;
	where Portfolio_Year >= &yrTSStart;
run;

*Delete Intermediate datasets related to Breakpoint calculation;
proc datasets noprint; 	delete 	data_breakpoint_bm
								data_breakpoint_inv
								data_breakpoint_mcap
								data_breakpoint_op
								stat_bm_percentiles
								stat_inv_percentiles
								stat_mcap_june
								stat_op_percentiles
								Breakpoint_firms
								Mcap_PY
								Mcap_June;
quit;

*********************************************
Directly read FF breakpoints
*********************************************;
/*proc import out = Summary_Breakpoints*/
/*	datafile = "C:\Udit\Research Affiliates\Output\FF_Breakpoints.xlsx"*/
/*	dbms = xlsx;*/
/*	getnames = yes;*/
/*run;*/


/*(3.2) Create Portfolio Master - Data for NYSE, ASE, NASDAQ stocks for all years*/
data PortfolioMaster (Drop = PDDUR AT COGS REVT XINT XSGA AT_1 
							 CUSIP GVKEY DATADATE PERMCO YEAR MONTH);
	set Fundamentals;
	where (EXCHCD in (1,2,3,31,32,33) ) and
		  (line_gvkey > 0); 			*Unlike FF (1993) paper where line_gvkey > 1 ;
		 							
									*1 is for New York Stock Exchange;
									*2 is for American Stock Exchange;
									*3 is for NASDAQ Stock Market;
		 							*31,32,33 helps to capture those months when stock
									 is trading, but undergoing e.g. reorganization
		 							 e.g. PERMNO 69075; 
run;
/*Note: Don't use COMPUSTAT 'EXCHG' to filter over (11, 12, 14) in the step above*/

*Arrange columns;
data PortfolioMaster;
	Retain PERMNO Portfolio_Year FYEAR  CONM TIC ff_profitability ff_inv ff_BM EXCHG;
	set PortfolioMaster;
run;


/*(3.3) Merge Breakpoint dataset with Portfolio Master*/
proc sort data = PortfolioMaster; by Portfolio_Year PERMNO; run;

data PortfolioMaster;
	merge PortfolioMaster (in = k) Summary_breakpoints;
	by Portfolio_year;
	if k;
run;
	

/*(3.4) Merge CRSP with Portfolio Master 											*/
* ISSUE: Take care of duplicates - 'PERMNO Portfolio_Year' repeats in PortfolioMaster
  e.g. when company changes Financial Year during year;
/*proc sql;*/
/*	create table temp1 as*/
/*	select P.**/
/*	from PortfolioMaster as P */
/*		inner join ( select PERMNO, Portfolio_Year, count(*) as count*/
/*					 from PortfolioMaster*/
/*					 group by PERMNO, Portfolio_Year*/
/*				   ) as T*/
/*		on P.PERMNO = T.PERMNO and P.Portfolio_Year = T.Portfolio_Year*/
/*	where T.Count >1*/
/*	order by P.CONM;*/
/*quit;*/

* CORRECTION; 
proc sort data = PortfolioMaster; 		   by PERMNO descending FYEAR; run;	
							*So we keep the latest FYEAR data in next step;
proc sort data = PortfolioMaster nodupkey; by PERMNO Portfolio_Year; run; 
														*Removes ~500 obs;

proc sort data = ReturnsAdj; by PERMNO Portfolio_Year Portfolio_Month; run;
data PortfolioReturns;
	merge PortfolioMaster (in = k) ReturnsAdj;
	by PERMNO Portfolio_Year;
	if k;
run;

/*(3.5) Subset to create Fama French Portfolios									*/

* Delete rows corresponding to stocks that delist by June End. 
  e.g. If a firm delists in June 1985, it will appear in portfolio for 1986,
  since it will pass all the necessary filters. However, it will have no return 
  data starting July;
* Also prune dataset, to keep data for the same period as the desired time series output;
data PortfolioReturns;
	set PortfolioReturns;
	where missing(Portfolio_month) = 0 and 
		  Portfolio_Year >= &yrTSStart and
		  Portfolio_Year <  &yrTSEnd;
run;

* Create Separate PortfolioReturns datasets for different factors;
data PortfolioReturnsRM;
	set PortfolioReturns;
	where missing(Mcap_June)     = 0;
run;

data PortfolioReturnsHML;
	set PortfolioReturns;

	*stocks for which we have market equity data for December of t-1 and 
	June of t, and (positive) book equity data for t-1.;
	where missing(Mcap_June)     = 0 and	missing(Mcap_PY)	= 0 and
	      missing(BE)        	 = 0;
run;

data PortfolioReturnsRMW;
	set PortfolioReturns;

	*stocks for which we have market equity data for June of t, 
	(positive) book equity data for t-1, non-missing revenues data for t-1, 
	and non-missing data for at least one of the following: COGS, SG&A, or 
	Int. expense t-1.;
	where missing(Mcap_June)     = 0 and	missing(BE)        	 = 0 and	
		  missing(ff_profitability) = 0;
run;

data PortfolioReturnsCMA;
	set PortfolioReturns;

	*stocks for which we have market equity data for June of t 
	 and total assets data for t-2 and t-1.;
	where missing(Mcap_June)     = 0 and	missing(ff_inv)		= 0;	
run;
											
*Value - 6 porfolios;
data ffValue_bg 	ffValue_bn 		ffValue_bv 
	 ffValue_sg 	ffValue_sn  	ffValue_sv;  
	set PortfolioReturnsHML; 
	if MCap_June => MCAP50 then do;
			     if (ff_BM <  BM30) 			       then output ffValue_bg;
			else if (ff_BM >= BM30) and (ff_BM < BM70) then output ffValue_bn;
			else if (ff_BM >=  BM70) 				   then output ffValue_bv;
		end;
	else if ( MCap_June <  MCAP50 ) then do;
				 if (ff_BM <  BM30) 				   then output ffValue_sg;
			else if (ff_BM >= BM30) and (ff_BM < BM70) then output ffValue_sn;
			else if (ff_BM >=  BM70) 				   then output ffValue_sv;
	end;
	keep Permco Permno Portfolio_Year Portfolio_Month MCap_June MCap_June_sum MCap
		 RET RETX;
run;

*Profitability - 6 porfolios;
data ffProfit_bw 	ffProfit_bn 		ffProfit_br 
	 ffProfit_sw 	ffProfit_sn  		ffProfit_sr;  
	set PortfolioReturnsRMW; 
	if MCap_June => MCAP50 then do;
			     if (ff_PROFITABILITY <  OP30) 			    				 then output ffProfit_bw;
			else if (ff_PROFITABILITY >= OP30) and (ff_PROFITABILITY < OP70) then output ffProfit_bn;
			else if (ff_PROFITABILITY >=  OP70) 				    		 then output ffProfit_br;
		end;
	else if ( MCap_June <  MCAP50 ) then do;
				 if (ff_PROFITABILITY <  OP30) 								 then output ffProfit_sw;
			else if (ff_PROFITABILITY >= OP30) and (ff_PROFITABILITY < OP70) then output ffProfit_sn;
			else if (ff_PROFITABILITY >=  OP70) 				    		 then output ffProfit_sr;
	end;
	keep Permco Permno Portfolio_Year Portfolio_Month MCap_June MCap_June_sum MCap
		 RET RETX;
run;

*Investments - 6 porfolios;
data ffInv_bc 	ffInv_bn 		ffInv_ba 
	 ffInv_sc 	ffInv_sn  		ffInv_sa;  
	set PortfolioReturnsCMA; 
	if MCap_June => MCAP50 then do;
			     if (ff_INV <  INV30) 			    	   then output ffInv_bc;
			else if (ff_INV >= INV30) and (ff_INV < INV70) then output ffInv_bn;
			else if (ff_INV >=  INV70) 					   then output ffInv_ba;
		end;
	else if ( MCap_June <  MCAP50 ) then do;
				 if (ff_INV <  INV30) 					   then output ffInv_sc;
			else if (ff_INV >= INV30) and (ff_INV < INV70) then output ffInv_sn;
			else if (ff_INV >=  INV70) 				       then output ffInv_sa;
	end;
	keep Permco Permno Portfolio_Year Portfolio_Month MCap_June MCap_June_sum MCap
		 RET RETX;
run;


/*(3.6) Create return time series for all the above portfolio (using a MACRO)					*/
%macro returns_monthly (datain = );
/* 
DATAIN 				- input data set
Weighting Scheme	- Value Weighted
Rebalance Frequency - Annual (normalized monthly)*/

/*%let datain = ffInv_sa;*/

	*1) List of Stocks available on July 1st of each Portfolio Year;
	proc sql;
		create table PortfolioJuly as
		select Portfolio_Year, Permno,  Mcap_June as port_wt
		from &datain
		where Portfolio_Month = 1
		order by Portfolio_Year;
	quit;
	
	*2) Using above output, sort Ret_Comp dataset;
	proc sql;
		create table Ret_Temp as
		select R.*, J.port_wt
		from PortfolioJuly J inner join 
			 &datain R
		on  J.Portfolio_Year = R.Portfolio_Year
		and J.Permno         = R.Permno
		order by PERMNO, Portfolio_YEAR, Portfolio_MONTH;
	quit;
	
	*2a) Stores the number of firms in the portfolio each month;
	proc sql;
		create table temp as
		select Portfolio_year, Portfolio_Month, count(distinct PERMNO) as &datain
		from Ret_Temp
		group by Portfolio_year, Portfolio_Month;
	quit;

	*2b);
	data Portfolio_firms;
		merge Portfolio_firms temp;
		by Portfolio_year Portfolio_Month;
	run;

	*3) Calculate Drifted weights;
	* Is a stock stops trading for few months, and then comes back in 
	  another month (e.g. PERMNO 11039 in 1990). For the month of re-entry
	  we will have missing values for Ret and Retx.;	
	data Ret_Temp;
		set Ret_Temp;
		if missing(RET) then RET = 0;
		if missing(RETX) then RETX =0;
	run;

	*4);
	data Ret_Temp;
		set Ret_Temp;
		by PERMNO Portfolio_YEAR Portfolio_MONTH;
		lagretx = lag(retx);		*Drift happens only due to price appreciation;
		if (first.PERMNO) or (Portfolio_Month = 1) then lagretx = 0;
	run;

	*5); 
	data Ret_Temp;
		set Ret_Temp;
		retain dyn_wt;
		by PERMNO Portfolio_YEAR Portfolio_MONTH;
		if (Portfolio_Month = 1)	then dyn_wt = port_wt;		
		else 	 						 dyn_wt = dyn_wt*(1+lagretx);
	run;

	*6) Re-normalize Drifted weights;
	proc sort data = Ret_Temp; by Portfolio_YEAR Portfolio_MONTH; run;

	*7);
	proc means data = Ret_Temp noprint;
		var dyn_wt;
		by Portfolio_YEAR Portfolio_MONTH;
		output out = Stat_sumwt sum = sumwt;
	run;

	*8);
	data _null_;
  		slept = sleep(1,0.01);
	run;
	

	data Stat_sumwt;
		set Stat_sumwt;
		where missing(Portfolio_month) = 0;
	run;

	*9);
	data Ret_Temp (drop = _FREQ_ _TYPE_ sumwt);
		merge Ret_Temp (in = k) Stat_sumwt;
		by Portfolio_YEAR Portfolio_MONTH;
		dyn_wt = dyn_wt/sumwt;
		if k;
	run;

	*10) Calculate Returns;
	proc means data = Ret_Temp noprint;
		var ret;						*Total returns (including distributions);
		weight dyn_wt;
		by Portfolio_YEAR Portfolio_MONTH;
		output out = ret_&datain (drop = _TYPE_) mean = TR_&datain;
	run;

	*11);
	data _null_;
  		slept = sleep(1,0.01);
	run;
	
	data ret_&datain;
		set ret_&datain;
		if missing(Portfolio_MONTH) then delete;
	run;

	*12) Delete temporary datasets;
	proc datasets noprint;	delete PortfolioJuly Ret_Temp Stat_Sumwt temp; 	quit;

%mend returns_monthly;

data Portfolio_firms;
	set ffValue_bg;
	Keep portfolio_year portfolio_month;
	delete;
run;


*Market - 1 portfolio;
%returns_monthly (datain = PortfolioReturnsRM);

*Value - 6 porfolios;
%returns_monthly (datain = ffValue_sg);
%returns_monthly (datain = ffValue_sn);
%returns_monthly (datain = ffValue_sv);
%returns_monthly (datain = ffValue_bg);
%returns_monthly (datain = ffValue_bn);
%returns_monthly (datain = ffValue_bv);


*Profitability - 6 porfolios;
%returns_monthly (datain = ffProfit_sw);
%returns_monthly (datain = ffProfit_sn);
%returns_monthly (datain = ffProfit_sr);
%returns_monthly (datain = ffProfit_bw);
%returns_monthly (datain = ffProfit_bn);
%returns_monthly (datain = ffProfit_br);


*Investments - 6 porfolios;
%returns_monthly (datain = ffInv_sc);
%returns_monthly (datain = ffInv_sn);
%returns_monthly (datain = ffInv_sa);
%returns_monthly (datain = ffInv_bc);
%returns_monthly (datain = ffInv_bn);
%returns_monthly (datain = ffInv_ba);


* Create Summary Datasets;
data Summary_ff_subPortfolios (drop = _FREQ_);	
	merge	ret_ffValue_sg	ret_ffValue_sn	ret_ffValue_sv
			ret_ffValue_bg	ret_ffValue_bn	ret_ffValue_bv
		
			ret_ffProfit_sw	ret_ffProfit_sn	ret_ffProfit_sr
			ret_ffProfit_bw	ret_ffProfit_bn	ret_ffProfit_br

			ret_ffInv_sc	ret_ffInv_sn	ret_ffInv_sa
			ret_ffInv_bc	ret_ffInv_bn	ret_ffInv_ba

			ret_portfolioreturnsrm;

	by Portfolio_YEAR Portfolio_MONTH;
run;


/*(3.7) Create return time series for Factor - Total Returns including distributions*/
%Let Inv = TR_ffInv;
%Let OP  = TR_ffProfit;
%Let BM  = TR_ffValue;

data Summary_ff_factors;
	set Summary_ff_subPortfolios;
	SMB = (   (&BM._sg  + &BM._sn  + &BM._sv  - &BM._bg  - &BM._bn  - &BM._bv )/3
		    + (&OP._sr  + &OP._sn  + &OP._sw  - &OP._br  - &OP._bn  - &OP._bw )/3	
			+ (&Inv._sc + &Inv._sn + &Inv._sa - &Inv._bc - &Inv._bn - &Inv._ba)/3
           )/3;

	HML = (&BM._sv  + &BM._bv  - &BM._sg  - &BM._bg )/2;
	RMW = (&OP._sr  + &OP._br  - &OP._sw  - &OP._bw )/2;
	CMA = (&Inv._sc + &Inv._bc - &Inv._sa - &Inv._ba)/2;
	RM  = TR_PortfolioReturnsRM;

	Keep Portfolio_Year Portfolio_Month SMB HML RMW CMA RM;
run;	

* Delete intermediate datasets;
proc datasets noprint;	
	delete  PortfolioReturnsRM	PortfolioReturnsHML	PortfolioReturnsCMA PortfolioReturnsRMW
			ffValue_bg	ffValue_bn	ffValue_bv	ffValue_sg	ffValue_sn	ffValue_sv
			ffProfit_bw	ffProfit_bn	ffProfit_br	ffProfit_sw	ffProfit_sn	ffProfit_sr
			ffInv_bc	ffInv_bn	ffInv_ba	ffInv_sc	ffInv_sn	ffInv_sa
			ret_ffValue_bg	ret_ffValue_bn	ret_ffValue_bv	ret_ffValue_sg	ret_ffValue_sn	ret_ffValue_sv
			ret_ffProfit_bw	ret_ffProfit_bn	ret_ffProfit_br	ret_ffProfit_sw	ret_ffProfit_sn	ret_ffProfit_sr
			ret_ffInv_bc	ret_ffInv_bn	ret_ffInv_ba	ret_ffInv_sc	ret_ffInv_sn	ret_ffInv_sa
			ret_PortfolioReturnsRM;
quit;

* Print output to excel;
proc export data = Summary_breakpoints
	outfile= "\\dev-sasbi101\temp\Gupta\Output\Summary_breakpoints" 
	dbms = xlsx; 
run;

proc export data = Summary_ff_factors 
	outfile="&output.\Summary_ff_factors" 
	dbms = xlsx
	replace; 
run;

proc export data = Summary_ff_subPortfolios 
	outfile="&output.\Summary_ff_subPortfolios" 
	dbms = xlsx
	replace; 
run;

proc export data = Portfolio_firms 
	outfile="&output.\Summary_Portfolio_firms" 
	dbms = xlsx
 	replace; 
run;

proc export data = Portfolio_firms 
	outfile="&output.\Summary_Portfolio_firms" 
	dbms = xlsx
	replace; 
run;













************************************************************************
Replicating Carhart's Momentum factor
- Create the Cummulative Return variable
- Rank and create 6 portfolio based on Cummulative Return and Size
- Create Monthly return series for each sub-portoflio
- Summarize
- Create a factor return
************************************************************************;
proc sort data = ReturnsAdj; by PERMNO Portfolio_Year Portfolio_Month; run; 

data PortfolioReturnsMom;
	set ReturnsAdj;
	by PERMNO Portfolio_Year Portfolio_Month;

	retain line;
	if first.PERMNO then line = 1;
	else 				 line = line + 1;

	cum_ret = 0;
	MCap_1  = lag(MCapAdj);		*Lagged MCap is used to calculate the 
								 portfolio weight for that month;

	Keep PERMCO PERMNO Portfolio_Year Portfolio_Month
		 EXCHCD RET RETX MCapAdj MCap_1 line cum_ret;
run;

* Create Cummulative Return variable - from t-12 to t-2;
%MACRO GetCummRet;
	%do iter = 2 %to 12;
	data PortfolioReturnsMom;
		set PortfolioReturnsMom;
		by PERMNO Portfolio_Year Portfolio_Month;					
		cum_ret = (1+cum_ret)*(1+lag&iter(ret))-1;
	run;
	%end;
%mend GetCummRet;
%GetCummRet;

data PortfolioReturnsMom;
	set PortfolioReturnsMom;
	if line >=13;
run;

* Calculate BreakPoints using NYSE data;
data data_breakpoint_Mom;
	set PortfolioReturnsMom;
	where EXCHCD = 1;
run;

proc sort data  = data_breakpoint_Mom; by Portfolio_Year Portfolio_Month; run;
proc univariate data = data_breakpoint_Mom noprint;
	var cum_ret ;
	by Portfolio_Year Portfolio_Month;
	output out = Stat_Mom_percentiles
	pctlpts = 30 70 pctlpre = Mom;
run; 

proc univariate data = data_breakpoint_Mom noprint;
	var MCap_1 ;
	by Portfolio_Year Portfolio_Month;
	output out = Stat_MCap_median
	pctlpts = 50 pctlpre = Mcap;
run; 

data Summary_mom_breakpoints;
	merge Stat_Mom_percentiles Stat_MCap_median;
	by Portfolio_Year Portfolio_Month;
run;

* Merge Breakpoints;
proc sort data = PortfolioReturnsMom; by Portfolio_Year Portfolio_Month; run; 

data PortfolioReturnsMom;
	merge PortfolioReturnsMom Summary_mom_breakpoints;
	by Portfolio_Year Portfolio_Month;
run;

* Delete intermediate datasets;
proc datasets noprint; delete Stat_Mom_percentiles Stat_MCap_median Summary_mom_breakpoints; run;

* Create Monthly 'Sum of Market Cap' variable;
proc means data = PortfolioReturnsMom noprint;
	var MCap_1;
	by Portfolio_YEAR Portfolio_MONTH;
	output out = Stat_sum_MCap sum = sumMCap;
run;
	
* Merge the variable & create weights;
data PortfolioReturnsMom (drop = _FREQ_ _TYPE_);
	merge PortfolioReturnsMom (in = k) Stat_sum_MCap;
	by Portfolio_YEAR Portfolio_MONTH;
	if k;
run;

*Momentum - 6 porfolios;
data ffMom_bh 	ffMom_bn 	ffMom_bl 
	 ffMom_sh 	ffMom_sn  	ffMom_sl;  
	set PortfolioReturnsMom; 
	if MCap_1 => MCAP50 then do;
			     if (cum_ret >= Mom70) 			    then output ffMom_bh;
			else if (cum_ret <  Mom30) 				then output ffMom_bl;
			else 				   					     output ffMom_bn;
		end;
	else if ( MCap_1 <  MCAP50 ) then do;
				 if (cum_ret >= Mom70) 				then output ffMom_sh;
			else if (cum_ret <  Mom30) 			 	then output ffMom_sl;
			else									     output ffMom_sn;
	end;
run;


%macro returns_monthly_momentum (datain = );
/* 
DATAIN 				- input data set
Weighting Scheme	- Value Weighted
Rebalance Frequency - Monthly 			*/

	*Read the data;
	data retcomp; set &datain;	run;

	*Calculate Dynamic weights - will not add up to 1 for sub-portfolios;	
	data retcomp;
		set retcomp;
		dyn_wt = MCap_1/sumMCap;
	run;

	*Normalize Weights each month - ensures weights add up to 1;
	proc sort data = retcomp; by Portfolio_YEAR Portfolio_MONTH; run;

	proc means data = retcomp noprint;
		var dyn_wt;
		by Portfolio_YEAR Portfolio_MONTH;
		output out = Stat_sumwt sum = sumwt;
	run;

	data Stat_sumwt;
		set Stat_sumwt;
		where missing(Portfolio_month) = 0;
	run;

	data retcomp (drop = _FREQ_ _TYPE_ sumwt);
		merge retcomp Stat_sumwt;
		by Portfolio_YEAR Portfolio_MONTH;
		dyn_wt = dyn_wt/sumwt;
	run;

	*Calculate Returns;
	proc means data = retcomp noprint;
		var ret;						*Total returns (including distributions);
		weight dyn_wt;
		by Portfolio_YEAR Portfolio_MONTH;
		output out = ret_&datain (drop = _TYPE_) mean = TR_&datain;
	run;

	data ret_&datain;
		set ret_&datain;
		if missing(Portfolio_MONTH) then delete;
	run;

	*Delete temporary datasets;
	proc datasets noprint;	delete retcomp stat_sumwt; 	quit;

%mend returns_monthly_momentum;

*Momentum - 6 porfolios;
%returns_monthly_momentum (datain = ffMom_bh);
%returns_monthly_momentum (datain = ffMom_bn);
%returns_monthly_momentum (datain = ffMom_bl);
%returns_monthly_momentum (datain = ffMom_sh);
%returns_monthly_momentum (datain = ffMom_sn);
%returns_monthly_momentum (datain = ffMom_sl);


data Summary_ff_Momentum (drop = _FREQ_);				*_FREQ_ field gets overwritten - only for indicative purpose;
	merge 	ret_ffMom_bh	ret_ffMom_bn	ret_ffMom_bl
			ret_ffMom_sh	ret_ffMom_sn	ret_ffMom_sl;
	by Portfolio_YEAR Portfolio_MONTH;
run;

data Summary_ff_Momentum;
	set Summary_ff_Momentum;
	Momentum = (TR_ffMom_sh + TR_ffMom_bh - TR_ffMom_sl - TR_ffMom_bl)/2;
run;


* Delete intermediate datasets;
proc datasets noprint;	
	delete  PortfolioReturnsMom	
			ffMom_bh	ffMom_bn	ffMom_bl	ffMom_sh	ffMom_sn	ffMom_sl
			ret_ffmom_bh	ret_ffmom_bn	ret_ffmom_bl	ret_ffmom_sh	ret_ffmom_sn	ret_ffmom_sl
			Stat_sum_MCap	data_breakpoint_Mom	;
quit;

*Export to excel;
proc export data = Summary_ff_Momentum 
	outfile="&Output.\Summary_ff_Momentum" 
	dbms = xlsx
	replace; 
run;
