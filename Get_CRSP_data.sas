/*
Prepare CRSP monthly datafile		
Comment				:	Using existing code by RA
Changes made		: 	Does not filter over SHRCD = 10, 11. 
						Retains PERMNO variable.
Output Dataset		:	CRSP_201505all												*/


* Path to output folder.;
libname DBfolder "\\dev-sasbi101\temp\Gupta\Datasets";
* Output file name.;
%let saveas = CRSP_201505all;

* Path to raw input.  Expecting raw CRSP data from this library.;
libname rawdat odbc dsn=FUNDAMENTALS schema=cs;
* Data as of... ;
%let asof = 2015-05-31;


*
- Load in raw data and rename columns to match with existing codes.  Note that WS_ID is named after a security ID.
- Fix WS_ID as string $9 to match with Datastream/Worldscope
- Convert data unit to millions
;

data crsp(keep= year month PERMNO WS_ID cusip SICCD Ticker EXCHCD TrdState return price shrout adjshares vol adjprice stockprice mktcap comnam ShrCd);
	set rawdat.CRSP_STOCK_DATA;
	where as_of_date = "&asof";
	year = input(substr(DATA_DATE, 1, 4), 8.);
	month = input(substr(DATA_DATE, 6, 2), 8.);

	WS_ID = substr(put(PERMNO, 9.), 5, 5);

	Shr = Shr/1000;   		* Shares outstanding from thousands to millions;
	Adjshr = Adjshr/1000;
	TCap = TCap/1000; 		* market cap from thousands to millions;
	Adjvol = Adjvol/10000;  * Adjusted volume from 100 shares to million shares;

	rename SICE = SICCD;
	* rename Tickere = Ticker;
	rename EXE = EXCHCD;
	rename Ret = return;
	rename Retx = price;
	rename Shr = shrout;
	rename Adjshr = adjshares;
	rename Adjvol = vol;
	rename Adjprc = adjprice;
	rename Prc = stockprice;
	rename TCap = mktcap;
	* rename Company_Name = comnam;
	rename SCE = ShrCd;

	format _all_ ;
	attrib _all_ label='';
run;

*
- Keep only the United States common stocks
- Delete missing returns
- Set stock price derived from average of bid and ask to positive
- Calculate market cap if it is missing and shares outstanding and share price are not
- Assign industry group base on French's 12-industry definition
;
data crsp; set crsp;
/*	if ShrCd in (10,11) and return >= -1;*/
	where return >= -1;

	adjprice = abs(adjprice);     * if price is negative, then that means it is derived from average of bit and ask;
	stockprice = abs(stockprice);
	if missing(adjprice) then adjprice=0;
	if missing(stockprice) then stockprice=0;
	if missing(adjshares) then adjshares=0;
	if missing(shrout) then shrout=0;
	if mktcap=0 and adjshares > 0 and adjprice > 0 then mktcap = adjshares * adjprice;
	if mktcap=0 and shrout> 0 and stockprice > 0 then mktcap = shrout * stockprice;
			
	if missing(SICCD) then grp=0; 
	if (SICCD>=100 & SICCD<=999) | (SICCD>=2000 & SICCD<=2399) | (SICCD>=2700 & SICCD<=2749) | (SICCD>=2770 & SICCD<=2799) | (SICCD>=3100 & SICCD<=3199) | (SICCD>=3940 & SICCD<=3989) then grp=1;
	if (SICCD>=2500 & SICCD<=2519) | (SICCD>=2590 & SICCD<=2599) | (SICCD>=3630 & SICCD<=3659) | (SICCD>=3710 & SICCD<=3711) | SICCD=3714 | SICCD=3716 | (SICCD>=3750 & SICCD<=3751) | SICCD=3792 | (SICCD>=3900 & SICCD<=3939) | (SICCD>=3990 & SICCD<=3999) then grp=2; 
	if (SICCD>=2520 & SICCD<=2589) | (SICCD>=2600 & SICCD<=2699) | (SICCD>=2750 & SICCD<=2769) | (SICCD>=3000 & SICCD<=3099) | (SICCD>=3200 & SICCD<=3569) | (SICCD>=3580 & SICCD<=3629) | (SICCD>=3700 & SICCD<=3709) | (SICCD>=3712 & SICCD<=3713) | SICCD=3715 | (SICCD>=3717 & SICCD<=3749) | (SICCD>=3752 & SICCD<=3791) | (SICCD>=3793 & SICCD<=3799) | (SICCD>=3830 & SICCD<=3839) | (SICCD>=3860 & SICCD<=3899) then grp=3; 
	if (SICCD>=1200 & SICCD<=1399) | (SICCD>=2900 & SICCD<=2999) then grp=4; 
	if (SICCD>=2800 & SICCD<=2829) | (SICCD>=2840 & SICCD<=2899) then grp=5;
	if (SICCD>=3570 & SICCD<=3579) | (SICCD>=3660 & SICCD<=3692) | (SICCD>=3694 & SICCD<=3699) | (SICCD>=3810 & SICCD<=3829) | (SICCD>=7370 & SICCD<=7379) then grp=6;
	if (SICCD>=4800 & SICCD<=4899) then grp=7;
	if (SICCD>=4900 & SICCD<=4949) then grp=8;
	if (SICCD>=5000 & SICCD<=5999) | (SICCD>=7200 & SICCD<=7299) | (SICCD>=7600 & SICCD<=7699) then grp=9;
	if (SICCD>=2830 & SICCD<=2839) | SICCD=3693 | (SICCD>=3840 & SICCD<=3859) | (SICCD>=8000 & SICCD<=8099) then grp=10;
	if (SICCD>=6000 & SICCD<=6999) then grp=11;
	if missing(grp) then grp=12;
run;

	
*
- Correct SIC Code for permno 58827 (BANKAMERICA CORP)
;
data crsp; set crsp;
	if WS_ID = "58827" and mdy(month, 1, year) <= mdy(6,1,1976) then do;
		SICCD = 6025;
		grp = 11;
	end;
run;

*
- Sort, reorder columns, and write dataset to output folder
;
proc sort data=crsp; by PERMNO year month; run;
data DBfolder.&Saveas;
	retain PERMNO WS_ID cusip comnam Ticker SICCD grp ShrCd EXCHCD Trdstate year month return price mktcap shrout stockprice adjshares adjprice vol;
	set crsp;
run;

proc datasets noprint; delete CRSP; run;
