/*
Prepare CCOMPUSTAT datafile		
Comment				:	Using existing code by RA
Changes made		: 	Does not drop DefTax variable.
						The CRSP dataset used to match includes all ShareCodes
Output Dataset		:	Compustat_201505all												*/


* Path to raw input.  Expecting compustat raw data and CRSP CCM link file.;
libname rawdat odbc dsn=FUNDAMENTALS schema=cs;
* Path to cleaned output;
libname cleandat "\\dev-sasbi101\temp\Gupta\Datasets";

* Cleaned dataset name;
%let crsp_cleaned = CRSP_201505all; * Input to this script, located in library cleandat;
%let compustat_cleaned = COMPUSTAT_201505all; * Output of this script;
* Data as of... ;
%let asof = 2015-05-31;


*
- Load in CLEANED CRSP dataset
;
data crsp; set cleandat.&crsp_cleaned; run;


*
- Load in CCM/CRSP Linking file
- Remove securites that do not have a valid link to Compustat
- If there's multiple links, keep only primary shares
  FYI: "P" = primary identified by COMPUSTAT, "C" = primary assigned by CRSP to resolve overlapping or missing primary from COMPUSTAT
  Tzee (June 2011) verified that "P" and "C" do not appear together at the same time
- Assign ID base on the security ID, fix it as string $9 to match with Datastream/Worldscope
;
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
/*
data link(drop=DATE);
	infile "K:\Constituent Data\CRSPRawData\201111\id_mapping.txt" firstobs=2 dlm='|' lrecl=5000 DSD missover;
	length ENTITY $6. DATE $8. IID $3. LINKPRIM $8. LINKTYPE $8.;
	input ENTITY$ DATE$ LINKID GVKEY IID$ LINKDT LINKENDDT PERMNO PERMCO LINKPRIM$ LINKTYPE$ USEDFLAG;

	year = input(substr(DATE,1,4), 8.);
	month = input(substr(DATE,5,2), 8.);
	WS_ID = put(ENTITY, 9.);
	if GVKEY<1 then delete;
run;
*/
proc sort data=link; by gvkey year month; run;
data link; set link;
	by gvkey year month;
	if (first.month and last.month) or linkprim in ("P","C");
run;


*
- Load in COMPUSTAT raw data and rename columns to match with existing codes.  Note that WS_ID is named after a security ID.
- Fix WS_ID as string $9 to match with Datastream/Worldscope
- Remove ETFs, title records and ADRs
- Derive fiscal year-end year and month
;

data compustat; set rawdat.FINANCIALS(rename=(gvkey=gvkey_str));
	if as_of_date = "&asof";
	if missing(ceql) = 0 or missing(xint) = 0 or missing(che) = 0; * characteristics of ETFs that show up;
	if dvc < 19000000 and emp < 19000000; * remove title records;
	if missing(adrr); * remove ADRs;

	format FYE mmddyy10.;
	if FYRA < 6 then FYE=intnx('month',mdy(fyra,1,fyyyy+1),0,'end');
	else FYE=intnx('month',mdy(fyra,1,fyyyy),0,'end');
	fye_y = year(FYE);
	
	AJEX = 1/AJEX;  * reciprocal to match with Worldscope definition;

	* rename kygvkey = gvkey;		gvkey = input(gvkey_str, 8.);	rename fyyyy = year;			rename fyra = fye_m;			rename XINT = interest;
	rename DD1 = debt1;				rename CHE = CSI;				rename ACT = CA;				rename LCT = CL;
	rename AT = assets;				rename DLTT = LTD;				rename PSTK = PS;				rename INTAN = INTANG;
	rename DP = Dep;				rename MIB = MI;				rename DLC = STD;				rename CEQ = BV;
	rename TXP = TP;				rename CSHO = SHROUT;			* rename CSHO = SHROUT_C;
	rename ACO = OTHERCA;			rename RECT = AR;				rename INVT = INV;				rename PPENT = PPE;
	rename AO = OTHERLTA;			rename AP = AP;					rename LCO = OTHERCL;			rename LO = OTHERLTL;
	rename SALE = sales;			rename CEQL = book;				rename OIBDP = cashflow;		rename DVC = div;
	rename EMP = emp;				rename XLR = wage;				rename IDIT = int_inc;			rename SPPE = sales_investments;
	rename SIV = sales_property;	rename CH = cash;				rename OANCF = opcashflow;		rename COGS = COGS;
	rename CAPX = Capex;			rename ADRR = ADR_Ratio;		rename LT = liability;
	* rename VARIABL0 = liability;	rename PSTKL = PSTKL;			rename GDWL = goodwill;			rename TXDITC = defTax;
	rename EPSFX = EPS_DILUTED;		rename CSHFD = ShareC_DILUTED;	rename IBADJ = NI_EPS;			rename IB = nibex;
	rename PRSTKC = RepurchaseCash;	rename SSTK = StockSaleCash;	rename TSTKME = MemoEntry;		rename TSTK = TreaStockTotal;
	rename TSTKC = TreaStockCommon;	rename NOPIO = nonopincome;		rename SPI = extr_pretax;		rename AJEX = ADJ_FACTOR;
	
	drop CONSOL DATAFMT INDFMT POPSRC AS_OF_DATE UPDATE_DATE gvkey_str;

	format _all_ ;
	informat _all_;
	format fye mmddyy10.;
	attrib _all_ label='';
run;


*
- Correct huge jumps in fundamentals in 1972, waiting for vendor's response (STAR BANC CORP)
;
data compustat; set compustat;
	if gvkey = 4723 and year = 1972 then do;
		cashflow = 14.73;
		sales = 48.33;
	end;
run;


*
- Merge CRSP & COMPUSTAT by matching primary shares permno as of FYE
- Records remaining in the "cleaned" COMPUSTAT are those with matching CRSP data only
;
proc sql;
	create table compustat0 as
	select f.*, s.WS_ID, s.comnam, s.ticker, s.cusip, s.SICCD, s.grp/*, s.shrout*/
	from (compustat f inner join link l	on f.gvkey = l.gvkey and f.fye_y = l.year and f.fye_m = l.month)
		inner join crsp s on l.WS_ID = s.WS_ID and l.year = s.year and l.month = s.month
	order by s.WS_ID, f.year
;quit;


*
- Since a record is now identified by security, there is a possibility that multiple records exist within the same fiscal year
  as a result of corporate actions.  In these cases, only the most recent record is retained.
;
proc sort data=compustat0; by WS_ID year fye; run;
data compustat0; set compustat0; by WS_ID year fye; if last.year; run;


*
- Calculate Russell-specific and other legacy factors
- REV, C_CAP and CASH_CAP are removed since this version (June 2011)
;
data compustat0; set compustat0;
	if not missing(TreaStockTotal) and TreaStockTotal ne 0 then TreaStockTot=TreaStockTotal; 
	else if not missing(MemoEntry) and MemoEntry ne 0 then TreaStockTot=MemoEntry; 
	else if TreaStockTotal=0 or MemoEntry=0 then TreaStockTot=0; 
	if not missing(TreaStockCommon) and TreaStockCommon ne 0 then TreasuryStock=TreaStockCommon; 
	else if not missing(TreaStockTot) and TreaStockTot ne 0 then TreasuryStock=TreaStockTot; 
	else if TreaStockCommon=0 or TreaStockTot=0 then TreasuryStock=0; 
run;

proc sql;
	create table compustat as
	select c.*, l.CA as CA_1, l.CSI as CSI_1, l.CL as CL_1, l.STD as STD_1, l.TP as TP_1, l.defTAX as defTAX_1, l.TreasuryStock as TreasuryStock_1
	from compustat0 c left join compustat0 l on c.WS_ID = l.WS_ID and c.year = l.year+1
;quit;

proc sort data=compustat; by WS_ID year; run;
data compustat0; set compustat;
	by WS_ID year;

	dCA=CA-CA_1;
	dCSI=CSI-CSI_1;
	dCL=CL-CL_1;
	dSTD=STD-STD_1;
	dTP=TP-TP_1;
	ddefTAX=defTAX-defTAX_1;
	dTreasuryStock=TreasuryStock-TreasuryStock_1;
	
	if missing(dCA) then dCA=0;
	if missing(dCSI) then dCSI=0;
	if missing(dCL) then dCL=0;
	if missing(dSTD) then dSTD=0;
	if missing(dTP) then dTP=0;
	if missing(ddefTAX) then ddefTAX=0;
	
	if missing(CA) then CA=0;
	if missing(CL) then CL=0;
    if missing(CSI) then CSI=0;
	if missing(STD) then STD=0;
	if missing(nonopincome) then nonopincome=0;
	if missing(extr_pretax) then extr_pretax=0;
	
	if missing(PS) then PS=0;
	if missing(INTANG) then INTANG=0;
	if missing(Dep) then Dep=0;
	if missing(MI) then MI=0;
	if missing(LTD) then LTD=0;
	if missing(TP) then TP=0;
	
	if missing(INV) then INV=0;
	if missing(PPE) then PPE=0;
	if missing(OTHERLTA) then OTHERLTA=0;
	if missing(AP) then AP=0;
	if missing(OTHERCL) then OTHERCL=0;
	if missing(OTHERLTL) then OTHERLTL=0;
	
	if missing(int_inc) then int_inc=0;
	if missing(sales_investments) then sales_investments=0;
	* if missing(s2) then s2=0;
	if missing(sales_property) then sales_property=0;
	
	if missing(interest) then interest=0;
	if missing(debt1) then debt1=0;
	if missing(PSTKL) then PSTKL=0;
	if missing(goodwill) then goodwill=0;
	if missing(defTax) then defTax=0;
	
	* Add cashflow missing handling. Fill the most of the missing records for sector group 8 in 1960 ;
	if missing(cashflow) then cashflow=nibex+Dep; 
	
	* Add missing book handling by substitutes (12/2009) ;
	if missing(book) then do;
		if not missing(BV) then book=BV;
		else if not missing(assets) then do;
			if not missing(liability) then book=assets-liability-PSTKL-goodwill;
			else book=assets-(CL+LTD+OTHERLTL+defTax+MI)-PSTKL-goodwill;
		end;
	end;
	
run;


proc sql;
	create table compustat as
	select c.*, l.book as book_1, l.assets as assets_1
	from compustat0 c left join compustat0 l on c.WS_ID = l.WS_ID and c.year = l.year+1
;quit;

proc sort data=compustat; by WS_ID year; run;
data compustat0; set compustat;
	by WS_ID year;

	OA = assets - CSI;
	OL = assets - STD - LTD - MI - PS - BV;
	NOA = (OA - OL)/assets_1;
	NOA_alt = (AR + INV + OTHERCA + PPE + INTANG + OTHERLTA - AP - OTHERCL - OTHERLTL)/assets_1;
	Accrual = ((dCA-dCSI) - (dCL-dSTD-dTP) - Dep)/assets_1;
	* rev = sales + int_inc + s1 + s2 + s3;
	* cap = SHROUT_c*prc;
	if (interest+STD) ~= 0 then dcr = cashflow/(interest+STD);
	if BV ~= 0 then cash_BV = CSI/BV;
	cash_asset = CSI/assets;
	* cash_cap = CSI/cap;
	if BV ~= 0 then c_BV = cash/BV;
	c_asset = cash/assets;
	* c_cap = cash/cap;
	
	* Add Earnings_PE;
	NI_DILUTED_EPS = EPS_DILUTED * ShareC_DILUTED;
	if (missing(NI_DILUTED_EPS)=0 and NI_DILUTED_EPS ne 0) then Earnings_PE = NI_DILUTED_EPS;
	else if (missing(NI_EPS)=0 and NI_EPS ne 0) then Earnings_PE = NI_EPS;
	else if (missing(nibex)=0 and nibex ne 0) then Earnings_PE = nibex;
	
	if n(book, book_1) > 0 then book_avg2 = sum(book, book_1)/n(book, book_1);
	if n(assets, assets_1) > 0 then assets_avg2 = sum(assets, assets_1)/n(assets, assets_1);
	
	* Add capadj for adjsales_avg5;
	capadj = book_avg2/assets_avg2;
	if not missing(capadj) and capadj < 0 then capadj = 0;
	if capadj > 1 then capadj = 1;
	
	* Add cash flow for operating activities;
	CFO = nibex + dep - nonopincome - extr_pretax;
	if not first.WS_ID then do;
		if dCA ne 0 then CFO = CFO - (dCA-dCSI);
		if dCL ne 0 then CFO = CFO + (dCL-dSTD);
		CFO = CFO + ddefTAX;
	end;
	
	* Add Treasury Stock buyback;
	NetRepurchaseCash = RepurchaseCash - StockSaleCash;
	
	if not missing(dTreasuryStock) and not missing(NetRepurchaseCash) then do;
		if TreasuryStock=0 and TreasuryStock_1=0 then Repurchase = NetRepurchaseCash;		
		else if dTreasuryStock < NetRepurchaseCash then Repurchase = dTreasuryStock;
		else Repurchase = NetRepurchaseCash;
	end;
	
	drop liability PSTKL goodwill  NI_DILUTED_EPS EPS_DILUTED ShareC_DILUTED NI_EPS 
	CA_1 CSI_1 CL_1 STD_1 TP_1 defTAX_1 TreasuryStock_1 dCA dCSI dCL dSTD dTP ddefTAX dTreasuryStock
	TreaStockTotal MemoEntry TreaStockTot TreaStockCommon TreasuryStock RepurchaseCash StockSaleCash NetRepurchaseCash book_avg2 assets_avg2
	book_1 assets_1;*defTax;

run;

*
- Calculate 5-year average of fundamentals
;

data
	compustat1(rename= (book=book_1 cashflow=cashflow_1 div=div_1 sales=sales_1 /*rev=rev_1*/ emp=emp_1 Earnings_PE=Earnings_PE_1 assets=assets_1 wage=wage_1 CFO=CFO_1 Repurchase=Repurchase_1))
	compustat2(rename= (book=book_2 cashflow=cashflow_2 div=div_2 sales=sales_2 /*rev=rev_2*/ emp=emp_2 Earnings_PE=Earnings_PE_2 assets=assets_2 wage=wage_2 CFO=CFO_2 Repurchase=Repurchase_2))
	compustat3(rename= (book=book_3 cashflow=cashflow_3 div=div_3 sales=sales_3 /*rev=rev_3*/ emp=emp_3 Earnings_PE=Earnings_PE_3 assets=assets_3 wage=wage_3 CFO=CFO_3 Repurchase=Repurchase_3)) 
	compustat4(rename= (book=book_4 cashflow=cashflow_4 div=div_4 sales=sales_4 /*rev=rev_4*/ emp=emp_4 Earnings_PE=Earnings_PE_4 assets=assets_4 wage=wage_4 CFO=CFO_4 Repurchase=Repurchase_4));
	set compustat0(keep= WS_ID year book cashflow div sales /*rev*/ emp Earnings_PE assets wage CFO Repurchase);
	year = year+1; output compustat1;
	year = year+1; output compustat2;
	year = year+1; output compustat3;
	year = year+1; output compustat4;
run;

data compustat; merge compustat0(in=_k) compustat1 compustat2 compustat3 compustat4;
	by WS_ID year;
	if _k;
	if n(book, book_1, book_2, book_3, book_4) > 0 then book_avg5 = sum(book, book_1, book_2, book_3, book_4)/n(book, book_1, book_2, book_3, book_4);
	if n(cashflow, cashflow_1, cashflow_2, cashflow_3, cashflow_4) > 0 then cashflow_avg5 = sum(cashflow, cashflow_1, cashflow_2, cashflow_3, cashflow_4)/n(cashflow, cashflow_1, cashflow_2, cashflow_3, cashflow_4);
	if n(div, div_1, div_2, div_3, div_4) > 0 then div_avg5 = sum(div, div_1, div_2, div_3, div_4)/n(div, div_1, div_2, div_3, div_4);
	if n(sales, sales_1, sales_2, sales_3, sales_4) > 0 then sales_avg5 = sum(sales, sales_1, sales_2, sales_3, sales_4)/n(sales, sales_1, sales_2, sales_3, sales_4);
	* if n(rev, rev_1, rev_2, rev_3, rev_4) > 0 then rev_avg5 = sum(rev, rev_1, rev_2, rev_3, rev_4)/n(rev, rev_1, rev_2, rev_3, rev_4);
	if n(emp, emp_1, emp_2, emp_3, emp_4) > 0 then emp_avg5 = sum(emp, emp_1, emp_2, emp_3, emp_4)/n(emp, emp_1, emp_2, emp_3, emp_4);
	if n(Earnings_PE, Earnings_PE_1, Earnings_PE_2, Earnings_PE_3, Earnings_PE_4) > 0 then Earnings_PE_avg5 = sum(Earnings_PE, Earnings_PE_1, Earnings_PE_2, Earnings_PE_3, Earnings_PE_4)/n(Earnings_PE, Earnings_PE_1, Earnings_PE_2, Earnings_PE_3, Earnings_PE_4);
	if n(assets, assets_1, assets_2, assets_3, assets_4) > 0 then assets_avg5 = sum(assets, assets_1, assets_2, assets_3, assets_4)/n(assets, assets_1, assets_2, assets_3, assets_4);
	if n(wage, wage_1, wage_2, wage_3, wage_4) > 0 then wage_avg5 = sum(wage, wage_1, wage_2, wage_3, wage_4)/n(wage, wage_1, wage_2, wage_3, wage_4);
	if n(CFO, CFO_1, CFO_2, CFO_3, CFO_4) > 0 then CFO_avg5 = sum(CFO, CFO_1, CFO_2, CFO_3, CFO_4)/n(CFO, CFO_1, CFO_2, CFO_3, CFO_4);
	if n(Repurchase, Repurchase_1, Repurchase_2, Repurchase_3, Repurchase_4) > 0 then Repurchase_avg5 = sum(Repurchase, Repurchase_1, Repurchase_2, Repurchase_3, Repurchase_4)/n(Repurchase, Repurchase_1, Repurchase_2, Repurchase_3, Repurchase_4);

	if not missing(Repurchase_avg5) and Repurchase_avg5 < 0 then Repurchase_avg5 = 0;
  	
	if not missing(div_avg5) or not missing(Repurchase_avg5) then do;
		if missing(div_avg5) then ddbTS_avg5=Repurchase_avg5; 
		else if missing(Repurchase_avg5) then ddbTS_avg5=div_avg5; 
		else ddbTS_avg5=div_avg5+Repurchase_avg5; 
	end;
	
	drop book_1 book_2 book_3 book_4;
	drop cashflow_1 cashflow_2 cashflow_3 cashflow_4;
	drop div_1 div_2 div_3 div_4;
	drop sales_1 sales_2 sales_3 sales_4;
	* drop rev_1 rev_2 rev_3 rev_4;
	drop emp_1 emp_2 emp_3 emp_4;
	drop Earnings_PE_1 Earnings_PE_2 Earnings_PE_3 Earnings_PE_4;
	drop assets_1 assets_2 assets_3 assets_4;
	drop wage_1 wage_2 wage_3 wage_4;
	drop CFO_1 CFO_2 CFO_3 CFO_4;
	drop Repurchase_1 Repurchase_2 Repurchase_3 Repurchase_4;
run;


* Macro to fill missing values with most recently available;
%macro fill_missing(inds, outds, field);
	data temp;
	set &inds;
	run;

	* fill missing &field by stale financial;
	proc sort data=temp; by ws_id year; run;

	data &outds(drop=tempfield);
	set temp;
	by ws_id year;
	retain tempfield;
	if first.ws_id or missing(&field) = 0 then tempfield=&field;
	else &field = tempfield;
	run;

	* Remove the code of the backfilling of missing values by the first available record 12/2009;
%mend fill_missing;

*
Fill missing in number of employer
;
%fill_missing(compustat, compustat, emp);

*
Fill missing in capital adjustment factor: capadj
If historical data is not available, then fill with industry average
;
%fill_missing(compustat, compustat, capadj);

proc sort data=compustat; by year grp; run;
proc means data=compustat noprint;
	var capadj;
	by year grp;
	output out=industrial_mean(drop=_freq_ _type_) mean=inducapadj;
run;

proc sort data=compustat; by year grp; run;
data compustat(drop=inducapadj); 
	merge compustat industrial_mean;
	by year grp; 
	if missing(capadj) then capadj=inducapadj;
	adjsales_avg5 = capadj*sales_avg5;
run;


*
- Set missing factors to zero
;
data compustat; set compustat;
	if missing(sales_avg5) then sales_avg5=0;
	if missing(book_avg5) then book_avg5=0;
	*if missing(cap_avg5) then cap_avg5=0;
	if missing(div_avg5) then div_avg5=0;
	* if missing(rev_avg5) then rev_avg5=0;
	if missing(cashflow_avg5) then cashflow_avg5=0;
	if missing(emp_avg5) then emp_avg5=0;
	if missing(emp) then emp=0;
	if missing(asset_avg5) then asset_avg5=0;
	if missing(wage_avg5) then wage_avg5=0;
	if missing(adjsales_avg5) then adjsales_avg5=0;
	if missing(ddbTS_avg5) then ddbTS_avg5=0;
	if missing(CFO_avg5) then CFO_avg5=0;
run;


*
- Drop unnecessary columns
- Sort and output
;
data compustat; set compustat; 
	Nation="UNITED STATES";
	drop gvkey /*cap rev rev_avg5*/;
	drop fye_m fye_y;
	drop NOA_alt Accrual cash_BV cash_asset /*cash_cap*/ c_BV c_asset emp_avg5 asset_avg5 wage_avg5 CFO_avg5 Repurchase_avg5;
run;

proc sort data=compustat; by WS_ID year; run;
data cleandat.&compustat_cleaned;
	retain WS_ID cusip comnam Ticker SICCD grp nation year;
	set compustat;
run;



