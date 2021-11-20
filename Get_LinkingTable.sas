******************************************************************************
PURPOSE		: 	This program aims to create two files which will be used
 				with OptionMetrics dataset, to identify 1) Primary Securities,
 				and 2) Securities of type 'Ordinary Common Shares'.
INPUT		: 	CCM (CRSP/Compustat merged) linking table from RA's database
				CRSP monthly return file (created using RA's code)
OUTPUT		: 	Linktable 
				CRSP_Meta
COMMENTS	:	The linking table only gives the PERMNO information. We use 
				CRSP_Meta data file to augment it with CUSIP information. 
				CUSIPs are important because they are the only common key between 
				OptionMetrics and CRSP/Compustat datafiles.
				Other variables such as ShrCd and EXCHCD, are also taken from 
				CRSP_Meta data file, since they'll help in filtering securities.
AUTHOR		:	UDIT GUPTA (August, 2015)
*****************************************************************************

*****************************************************************************
PRELIMINARIES
****************************************************************************;
*CRSP CCM link file;
libname rawdat odbc dsn=FUNDAMENTALS schema=cs;

*Data as of... ;
%let asof = 2015-05-31;

*Path to input folder;
libname DBfolder '\\dev-sasbi101\temp\Gupta\Datasets';

*CRSP monthly file. Using existing RA code to pull data. 
 However, did not apply the SHRCD filter;
%let raw_CRSP 		= crsp_201505all;	

*****************************************************************************
PROCESS:
1: Load RA's CCM/CRSP linking file (use code from Compustat_cleanup)
2: Read CRSP Monthly data file
3: Add CUSIPs to Linking Table from CRSP monthly data file
****************************************************************************;

/*1: Load RA's CCM/CRSP linking file (use code from Compustat_cleanup)		*/
data link(keep= year month WS_ID LINKID GVKEY IID LINKDT LINKENDDT PERMNO PERMCO LINKPRIM LINKTYPE USEDFLAG);
	set rawdat.ID_MAPPING;
	where as_of_date = "&asof";
	year = input(substr(DATA_DATE, 1, 4), 8.);
	month = input(substr(DATA_DATE, 6, 2), 8.);
	WS_ID = substr(put(PERMNO, 9.), 5, 5);
	if UGVKEY < 1 then delete;

	gvkey = input(UGVKEY, 8.);
	rename ULINKID = LINKID;
	rename UIID = IID;
	rename ULINKDT = LINKDT;
	rename ULINKENDDT = LINKENDDT;
	rename UPERMCO = PERMCO;
	rename ULINKPRIM = LINKPRIM;
	rename ULINKTYPE = LINKTYPE;

	format _all_ ;
	attrib _all_ label='';
run;

proc sort data=link; by gvkey year month; run;
data link; set link;
	by gvkey year month;
	if (first.month and last.month) or linkprim in ("P","C");
run;


/*2: Read CRSP Monthly data file											*/
data CRSP_meta;
	set DBfolder.&raw_CRSP;
	keep	Permno CUSIP COMNAM Ticker ShrCd EXCHCD Year Month;
run;

proc sql;
	title 'Step 2: Missing values in CRSP_meta?';
	select nmiss(PERMNO) as PERMNO, nmiss(CUSIP) as CUSIP, 
		   nmiss(ShrCd) as ShrCd, nmiss(EXCHCD) as EXCHCD
	from CRSP_meta;	
		*Missing CUSIPs are probably in cases where no official CUSIP was assigned, 
		 and CRSP gave its own dummy CUSIP (of type ***99*9*). 
		 RA's CRSP monthly file however does not capture these dummy CUSIPs.		 
		 source: http://www.crsp.com/products/documentation/data-definitions-c#cusip-header;
quit;


/*3: Add CUSIPs to Linking Table from CRSP monthly data file				*/
proc sort data=link; 	  by PERMNO Year Month; run;
proc sort data=CRSP_meta; by PERMNO Year Month; run;

*LEFT JOIN on LinkTable;
*Primary key used: PERMNO + YEAR + MONTH;
proc sql;
	create table linktable as 
	select t1.*,
		   t2.CUSIP  as CRSP_CUSIP, t2.Comnam, t2.Ticker, t2.ShrCd, t2.EXCHCD
	from link as t1
	left join CRSP_meta as t2
		on t1.PERMNO = t2.PERMNO
		and t1.YEAR  = t2.YEAR
		and t1.MONTH = t2.MONTH;

	title 'Step 3: Missing values in linktable?';
	select nmiss(PERMNO) as PERMNO, nmiss(CRSP_CUSIP) as CRSP_CUSIP, 
		   nmiss(ShrCd) as ShrCd, nmiss(EXCHCD) as EXCHCD,
		   nmiss(LinkPrim) as LinkPrim
	from linktable;	
		*Missing values for ShrCd and EXCHCD are created, because CRSP 
		 file does not have data for listing month, while the link for that
		 month exists (on the last day of the month);
quit;


*Delete intermediate datasets;
proc datasets noprint;	delete link;	run;
