/*

PROJECT CODE :: RMIT

Overview of project:
Underlying book is a Residential Mortgage Book with Performing and Non Peforming Loans

Scope:
Provide advisory support to client on pricing the remaining book by:
 - 1. By commenting on completeness and accuracy of the data prepared by external IT vendor
 - 2. By highlighting issues to be fixed by external IT vendor
 - 3. prepaying static and dynamic analysis as would be shared with investors / rating agencies incl.
	a. Pre-payment curves based on quarterly cohorts - Static Analysis
	b. Under-payment & Writeoff ( balance movement) curves based on quarterly cohorts - static analysis
	c. Arrears Evolution on a month on month basis - dynamic analysis
	d. Roll Rate for the book on a month on month basis - dynamic analysis
	e. Balance Summary 
	f. Option Exercise by Borrowers
	g. Pre-payment month on month bifurcation based on fixed vs floating rate -  dynamic analysis
 - 4. ad-hoc analysis for clarifications to investors qa's or internal structured products team
 - 5. Cash flow modelling for the live book using estimates of pre-payment risk, default rates etc as calculated in 3.
 - 6. Prepare following to be shared with investors / Rating Agencies:
     a. Historical payment files with only relevant fields, and in scope loans (csv or txt format)
	 b. All the analysis done under scope item (3) + reasonable ad-hoc analysis , in an excel format


 This file covers, scope 3.
 Final pivot and group by are left to be done in excels, as they are more flexible.

 Tables:
  - Dynamic				: 20millions rows , primary key on loan id & Date
  - cohort_dates		: 200k rows		  , primary key Date 
  - live_option			: 70k  rows		  , primary key loanid 
  - exlcusion_criteria  : 40k  rows		  , primary key loanid 

*/




-- Insertion Query for
BULK INSERT Project.dbo.dynamic
FROM '\CSVS_SPLIT_BY_YEAR\*.csv' --This is CSV file
WITH ( format = 'CSV',FIELDTERMINATOR =',',rowterminator = '\n',FIRSTROW = 2)




--- "3.a Prepayment Curve "

Select LoanType
, Cohort
, CohortQuartersOnBook
, PaymentDue       = sum(PaymentDue)
, PaymentReceived  = sum(PaymentReceived)
, PrePayment       = sum(PrePayment) 
, EOMBalance         = sum(case when LastMontInQuarter= 1 then EOMBalance else 0 end)
, SOMBalance         = sum(case when FirstMonthInQuareter =1 then SOMBalance else 0 end)
from 
(
select Date 
, d.BaseLoanId            --- = SUBSTRING(LoanId,15,12)
, d.ProductId
, Cohort = cd.origination_quarter
, CohortQuartersOnBook = case when Origination_date is not null then datediff(qq,concat(datepart(YY,cd.Origination_date),'-',3*datepart(qq,cd.Origination_date),'-',1),Date) 
					else null end 
, QuarterOnBook        = datediff(qq,cd.Origination_date,Date)
, EOMBalance           = Balance + Arrears
, PaymentDue           = PRINCIPAL_DUE + INTEREST_DUE + OTHERDUE
, PaymentReceived      = ( --- The Payment Received is needed to be there only for the Actual Payments and not the accounting payments
						CASE 
							WHEN CLOSURE_TYPE in    (	
													'10 IN ESSERE',
													'05 CONTR. APERTO',
													'91 ESTINTA ANT.', 
													'90 ESTINTA',
													'08 PREDISP. CHIUSURA',
													'85 ESTINTA PER SURROGA'
													)
							THEN PRINCIPAL_RECEIVED + INTEREST_RECEIVED + OTHERDUE_RECEIVED + ONEOFFPAYMENT 
						ELSE 0
						
					END
					)

, SOMBalance = 
  CASE
	  WHEN 
		Date > '2012-07-31' and Lag(Balance+Arrears,1)  over (partition by LoanId order by date) is null
		THEN Balance+Arrears
	  --WHEN year(cd.Origination_date) = year(date) and month(cd.Origination_date)  = month(date) THEN cd.Origination_Balance
		ELSE Lag(Balance+Arrears,1) over ( partition by LoanId order by date)
  END
, PrePayment = 
(
CASE
	WHEN CLOSURE_TYPE not in  ( '10 IN ESSERE',
								'05 CONTR. APERTO',
								'91 ESTINTA ANT.', 
								'90 ESTINTA',
								'08 PREDISP. CHIUSURA',
								'85 ESTINTA PER SURROGA'
							   )
	THEN 0
	WHEN 
		PRINCIPAL_RECEIVED + INTEREST_RECEIVED + OTHERDUE_RECEIVED + ONEOFFPAYMENT  - PRINCIPAL_DUE - INTEREST_DUE - OTHERDUE > 0 THEN PRINCIPAL_RECEIVED + INTEREST_RECEIVED + OTHERDUE_RECEIVED + ONEOFFPAYMENT  - PRINCIPAL_DUE - INTEREST_DUE - OTHERDUE
	ELSE 0
END
)
,
CASE 
  WHEN cd.CHF_LOAN = 1 THEN 'CHF'
	ELSE 'PL'
END As LoanType
, FirstMonthInQuareter = ROW_NUMBER() over( partition by cd.BaseLoanId,(case when Origination_date is not null then datediff(qq,concat(datepart(YY,cd.Origination_date),'-',3*datepart(qq,cd.Origination_date),'-',1),date)--concat(datepart(YY,Date) ,'-',datepart(mm,Date),'-',1)) 
					else null end ) order by date)
, LastMontInQuarter   = ROW_NUMBER() over( partition by cd.BaseLoanId,(case when Origination_date is not null then datediff(qq,concat(datepart(YY,cd.Origination_date),'-',3*datepart(qq,cd.Origination_date),'-',1),date)--concat(datepart(YY,Date) ,'-',datepart(mm,Date),'-',1)) 
					else null end ) order by date desc)
from dynamic d
join cohort_dates cd
on cd.BaseLoanId = d.BaseLoanId --- cast(substring(d.loanid,15,12) as float)
left join exclusion_criteria ec --from exclusion_criteria
on ec.BaseLoanId = cd.BaseLoanId
where d.PRODUCTID = 6 and (cd.origination_date is not null) and subaccount in (0,1) and (SUBSTRING(LOANID_OLD,5,3) not in ('007','021') or LOANID_OLD is null) and (loanid not like '%006-00446%') and (loanid_old not like '%006-00446%' or LOANID_old is null) 
and ec.BaseLoanId is NUll
) dt
where dt.Date > '2012-10-01'
group by LoanType,Cohort,CohortQuartersOnBook



-- "3.b) Balance Movement"

select LoanType
, Cohort
, CohortQuartersOnBook
, EOMBalance         = sum(case when LastMontInQuarter= 1 then EOMBalance else 0 end)
, PaymentDue         = sum(PaymentDue)
, PaymentReceived    = sum(PaymentReceived)
, SOMBalance         = sum(case when FirstMonthInQuareter =1 then SOMBalance else 0 end)
, PrePayment         = sum(PrePayment)
, Charge_Off_Balance = sum(Charge_off_Balance)
, UnderPayment       = sum(UnderPayment)
from 
(
select Date 
, BaseLoanId            = d.BaseLoanId -- SUBSTRING(LoanId,15,12)
, d.ProductId
, Cohort = cd.origination_quarter
, CohortQuartersOnBook = case when Origination_date is not null then datediff(qq,concat(datepart(YY,cd.Origination_date),'-',3*datepart(qq,cd.Origination_date),'-',1),Date) 
					else null end  
/* case when Origination_date is not null then cast (datediff(dy,cd.Origination_date,Date) / 90 as int) + 1
					      else null end-- Diff in Yrs x 4 + Diff in Qtrs + 1 */ 
, QuarterOnBook        = datediff(qq,cd.Origination_date,Date)
, EOMBalance           = Balance + Arrears
, PaymentDue           = PRINCIPAL_DUE + INTEREST_DUE + OTHERDUE
, PaymentReceived      = ( --- The Payment Received is needed to be there only for the Actual Payments and not the accounting payments
						CASE 
							WHEN CLOSURE_TYPE in    (	
													'10 IN ESSERE',
													'05 CONTR. APERTO',
													'91 ESTINTA ANT.', 
													'90 ESTINTA',
													'08 PREDISP. CHIUSURA',
													'85 ESTINTA PER SURROGA'
													)
							THEN PRINCIPAL_RECEIVED + INTEREST_RECEIVED + OTHERDUE_RECEIVED + ONEOFFPAYMENT 
						ELSE 0
						
					END
					)

, SOMBalance = 
 ( CASE
  WHEN 
	Date > '2012-07-31' and Lag(Balance+Arrears,1)  over (partition by LoanId order by date) is null
	THEN Balance+Arrears
  --WHEN year(cd.Origination_date) = year(date) and month(cd.Origination_date)  = month(date) THEN cd.Origination_Balance
  ELSE Lag(Balance+Arrears,1) over ( partition by LoanId order by date)
  END
  )
, PrePayment = 
(
CASE
	WHEN CLOSURE_TYPE not in  ( '10 IN ESSERE',
								'05 CONTR. APERTO',
								'91 ESTINTA ANT.', 
								'90 ESTINTA',
								'08 PREDISP. CHIUSURA',
								'85 ESTINTA PER SURROGA'
							   )
	THEN 0
	WHEN 
		PRINCIPAL_RECEIVED + INTEREST_RECEIVED + OTHERDUE_RECEIVED + ONEOFFPAYMENT  - PRINCIPAL_DUE - INTEREST_DUE - OTHERDUE > 0 THEN PRINCIPAL_RECEIVED + INTEREST_RECEIVED + OTHERDUE_RECEIVED + ONEOFFPAYMENT  - PRINCIPAL_DUE - INTEREST_DUE - OTHERDUE
	ELSE 0
END
)
,
 UnderPayment = 
(
CASE
	WHEN CLOSURE_TYPE not in  ( '10 IN ESSERE',
								'05 CONTR. APERTO',
								'91 ESTINTA ANT.', 
								'90 ESTINTA',
								'08 PREDISP. CHIUSURA',
								'85 ESTINTA PER SURROGA'
							   )
	THEN 0
	WHEN 
		PRINCIPAL_DUE + INTEREST_DUE + OTHERDUE  - (PRINCIPAL_RECEIVED + INTEREST_RECEIVED + OTHERDUE_RECEIVED + ONEOFFPAYMENT) > 0 THEN PRINCIPAL_DUE + INTEREST_DUE + OTHERDUE  - (PRINCIPAL_RECEIVED + INTEREST_RECEIVED + OTHERDUE_RECEIVED + ONEOFFPAYMENT)
	ELSE 0
END
)
,
Charge_Off_Balance = 
(
CASE
	WHEN CLOSURE_TYPE in  ( '92 SOFFERENZA')
	THEN Lag(Balance+Arrears,1) over ( partition by LoanId order by date)
	ELSE 0
END
)
,
CASE 
	WHEN cd.CHF_LOAN = 1  THEN 'CHF'
--	WHEN INTRATEDES in ('FS ITABOR CHF 6 MESI','SARON COMP. 3 MESI','CH LIBOR CHF 6 MESI') THEN 'CHF'
	ELSE 'PL'
END As LoanType
, FirstMonthInQuareter = ROW_NUMBER() over( partition by cd.BaseLoanId,(case when Origination_date is not null then datediff(qq,concat(datepart(YY,cd.Origination_date),'-',3*datepart(qq,cd.Origination_date),'-',1),Date) 
					else null end ) order by date)
, LastMontInQuarter   = ROW_NUMBER() over( partition by cd.BaseLoanId,(case when Origination_date is not null then datediff(qq,concat(datepart(YY,cd.Origination_date),'-',3*datepart(qq,cd.Origination_date),'-',1),Date) 
					else null end ) order by date desc)
from dynamic d
join cohort_dates cd
on cd.BaseLoanId = d.BaseLoanId --- cast(substring(d.loanid,15,12) as float)
left join exclusion_criteria ec --from exclusion_criteria
on ec.BaseLoanId = cd.BaseLoanId
where d.PRODUCTID in (6)  and (cd.origination_date is not null) and subaccount in (0,1) and (SUBSTRING(LOANID_OLD,5,3) not in ('007','021') or LOANID_OLD is null) and (loanid not like '%006-00446%') and (loanid_old not like '%006-00446%' or LOANID_old is null) 
and ec.BaseLoanId is NUll
) dt
where dt.Date > '2012-10-01'
group by LoanType,Cohort,CohortQuartersOnBook



--- "3. d) Roll Rate"

	select Date = concat(year(tb.Date),'-',month(tb.Date),'-1')
	, LoanType = tb.LoanType
	, ArrearBuckets = tb.ArrearBuckets
	, ArrearBuckets_SOM = tb.ArrearBuckets_SOM
	, NLoans     = count(distinct(tb.BaseLoanId))
	, EOMTotalBalance = sum(tb.UPB)+sum(tb.BalanceInArrears)
	, SOMTotalBalance = sum(tb.UPBSOM)+sum(tb.BalanceInArrearsSOM)

	from 

	(
	select Date 
	, BaseLoanId = dynamic.BaseLoanId
	, ProductId 
	, UPB =    ( Case 
				When CLOSURE_TYPE in  ( '92 SOFFERENZA') 
					Then PRINCIPAL_RECEIVED				
				Else Balance
				End
				)
	, UPBSOM = lag(BALANCE,1) over (partition by cast(substring(loanid,15,12) as float) order by Date )
	, BalanceInArrearsSOM = lag(Arrears,1) over (partition by cast(substring(loanid,15,12) as float) order by Date )
	, BalanceInArrears  = Arrears
	, DPD = DPD
	, ArrearBuckets = 
	(
	Case 
		WHEN CLOSURE_TYPE in ('92 SOFFERENZA')   Then 'Charged-off'
		WHEN CLOSURE_TYPE in ('91 ESTINTA ANT.','85 ESTINTA PER SURROGA') THEN 'Early-Redemption'
		WHEN CLOSURE_TYPE in ('90 ESTINTA')      THEN 'Closed'
		WHEN DPD = 0 Then 'Performing'
		WHEN DPD  < 31  Then '1-30 Days in Arrears'
		WHEN DPD  < 61  Then '31-60 Days in Arrears'
		WHEN DPD  < 91  Then '61-90 Days in Arrears'
		WHEN DPD  < 181 Then '90-180 Days in Arrears'
		WHEN DPD  < 361 Then '180-360 Days in Arrears'
		WHEN DPD  >360  Then '360+ Days in Arrears'
		ELSE 'N/A'
	End
	)
	, ArrearBuckets_SOM = 
	(
	Case 
		WHEN lag(DPD,1) over (partition by dynamic.BaseLoanId order by Date ) = 0    Then 'Performing'
		WHEN lag(DPD,1) over (partition by dynamic.BaseLoanId order by Date ) < 31   Then '1-30 Days in Arrears'
		WHEN lag(DPD,1) over (partition by dynamic.BaseLoanId  order by Date ) < 61  Then '31-60 Days in Arrears'
		WHEN lag(DPD,1) over (partition by dynamic.BaseLoanId  order by Date ) < 91  Then '61-90 Days in Arrears'
		WHEN lag(DPD,1) over (partition by dynamic.BaseLoanId order by Date ) < 181  Then '90-180 Days in Arrears'
		WHEN lag(DPD,1) over (partition by dynamic.BaseLoanId order by Date ) < 361  Then '180-360 Days in Arrears'
		WHEN lag(DPD,1) over (partition by dynamic.BaseLoanId order by Date ) >360   Then '360+ Days in Arrears'
		ELSE 'N/A'
	End
	)
	,
	LoanType = 
	( CASE
		WHEN cd.CHF_LOAN = 1 THEN 'CHF'
		--WHEN INTRATEDES in ('FS ITABOR CHF 6 MESI','SARON COMP. 3 MESI','CH LIBOR CHF 6 MESI') THEN 'CHF'
	  ELSE 'PL'
	  END
	)
	from dynamic 
	join cohort_dates cd
	on cd.BaseLoanId = dynamic.BaseLoanId
	left join ( select * from exclusion_criteria where criteria ='SOLD') ec --from exclusion_criteria
	on ec.BaseLoanId = cd.BaseLoanId
	where date > '2012-10-31' and productid = 6 and SUBACCOUNT in (0,1) and ec.Criteria is Null 
	--- cast(substring(loanid,15,12) as float)  not in  (select * from DropIds) and
	) tb
	group by tb.Date,tb.LoanType,tb.ArrearBuckets,tb.ArrearBuckets_SOM


---- "3.c) Arrear Summary"
-- fixed for earlier months, the NPL balance has to be taken from last month balance

	select tb.Date
	, LoanType = tb.LoanType
	, ArrearBuckets = tb.ArrearBuckets
	, NLoans = count(distinct(tb.BaseLoanId))
	, UPB = sum(tb.UPB) + sum(case when tb.ArrearBuckets = 'Charged-off' then lagged_balance else 0 end)
	, Arrears = sum(tb.BalanceInArrears)
	, TotalBalance = sum(tb.UPB)+sum(tb.BalanceInArrears)

	from 
	(
	select Date 
	, BaseLoanId = dynamic.BASELOANID
	, ProductId 
	, LOANSTATUS
	, Lagged_Balance = Lag(Balance+Arrears,1) over ( partition by dynamic.LOANID order by date) 
	, UPB =  Balance
	, BalanceInArrears  = Arrears
	, DPD = DPD
	, ArrearBuckets = 
	(
	Case 
		WHEN CLOSURE_TYPE in  ( '92 SOFFERENZA') Then 'Charged-off'
		WHEN DPD  = 0  Then 'Performing'
		WHEN DPD  < 31  Then '1-30 Days in Arrears'
		WHEN DPD  < 61  Then '31-60 Days in Arrears'
		WHEN DPD  < 91  Then '61-90 Days in Arrears'
		WHEN DPD  < 181 Then '90-180 Days in Arrears'
		WHEN DPD  < 361 Then '180-360 Days in Arrears'
		WHEN DPD  >360  Then '360+ Days in Arrears'
		ELSE 'N/A'
	End
	)
	,
	LoanType = 
	( CASE
		WHEN INTRATEDES in ('FS ITABOR CHF 6 MESI','SARON COMP. 3 MESI','CH LIBOR CHF 6 MESI') THEN 'CHF'
	  ELSE 'PL'
	  END
	)

	from dynamic 
	left join ( select * from exclusion_criteria where criteria ='SOLD') ec --from exclusion_criteria
	on ec.BaseLoanId = dynamic.BaseLoanId
	--- cast(substring(loanid,15,12) as float)  not in  (select * from DropIds) and
	where date > '2012-08-30' and productid = 6 and ec.Criteria is Null 
	--- cast(substring(loanid,15,12) as float)  not in  (select * from DropIds) and
	) tb
	group by tb.Date,tb.LoanType,tb.ArrearBuckets

-- "3.e) Balance Summary"

	select CHF_LOAN,origination_quarter, origination_balance = sum(original_balance), Loan_counts = count(*) from cohort_dates d
	left join ( select * from exclusion_criteria where criteria ='SOLD') ec --from exclusion_criteria
	on ec.BaseLoanId = d.BaseLoanId
	where ec.BaseLoanId is Null
	group by CHF_LOAN,origination_quarter
	order by CHF_LOAN,origination_quarter

	-- Live Perimeter
	select Book_Category,Origination_Quarter,current_balance = sum(total_exposure) from live_perimeter
	group by Book_Category,Origination_Quarter
	order by Book_Category,Origination_Quarter


	-- Balance Check March 23
	Select date , d.BaseLoanId , balance = sum(balance+arrears) from dynamic d
	join lp_mar23 lp on	
	lp.BaseLoanId = d.BaseLoanId
	where date = '2023-03-31'
	group by date , d.BaseLoanId


	-- "6.a) Payment History Tape"

	select Date,BaseLoanId	, LoanID, LoanId_Old , INTRATETYPE, TOTALRATE , BASERATE,INTRATEDES,BALANCE,DRAWDOWN,ARREARS,DPD,PRINCIPAL_DUE,INTEREST_DUE,OTHERDUE,PRINCIPAL_RECEIVED,INTEREST_RECEIVED,OTHERDUE_RECEIVED,ONEOFFPAYMENT,WRITEOFF,NPL_FLAG,CLOSURE_TYPE,LOANCLOSEDATE
	from dynamic
	where PRODUCTID = 6	


-- "3.g) Pre-payment for fixed vs floating loans"

  Select Date , LoanType, Int_Rate_Type ,  PrincipalBalance = sum(PrincipalBalance) , TotalDue  = sum(TotalDue) , TotalReceived = sum(TotalReceived) , TotalArrear = sum(ARREARS)
  from (
  Select  Date , PrincipalBalance  = Balance , TotalDue = PRINCIPAL_DUE + INTEREST_DUE + OTHERDUE  , ARREARS
		, TotalReceived      = ( --- The Payment Received is needed to be there only for the Actual Payments and not the accounting payments
						CASE 
							WHEN CLOSURE_TYPE in    (	
													'10 IN ESSERE',
													'05 CONTR. APERTO',
													'91 ESTINTA ANT.', 
													'90 ESTINTA',
													'08 PREDISP. CHIUSURA',
													'85 ESTINTA PER SURROGA'
													)
							THEN PRINCIPAL_RECEIVED + INTEREST_RECEIVED + OTHERDUE_RECEIVED + ONEOFFPAYMENT 
						ELSE 0
						
					END
					)
  
	, Int_Rate_Type = 
	case 
		WHEN INTRATEDES in ('FS ITABOR CHF 6 MESI','SARON COMP. 3 MESI','CH LIBOR CHF 6 MESI') Then 'Variable_Rate'
		when INTRATETYPE in  ('CONGUAGLIO POSTICIP.','FISSO 10 ANNI (RIN)','FISSO 5 ANNI','TASSO FISSO')
				then 'Fixed_Rate' 
		when INTRATETYPE in ('BW VAR CON RIC CAPIT','MCQ RATA FIX DUR VAR','MCQ VAR CON RIC CAP.','RATA FIX - DURATAVAR','TASSO VARIABILE','VAR CON RIC CAP CERT','VAR CON RIC CAPITALE')
				then 'Variable_Rate'
	end 
,	LoanType = 
	( CASE
		WHEN INTRATEDES in ('FS ITABOR CHF 6 MESI','SARON COMP. 3 MESI','CH LIBOR CHF 6 MESI') THEN 'CHF'
	  ELSE 'PL'
	  END
	)
from dynamic
where PRODUCTID = 6
) t
group by Date, LoanType, Int_Rate_Type



--"3.f) Option Exercise by Borrowers"
select tt.yr_month,tt.RateType,tt.Previous_RateType,SOMBalance = sum(tt.SOMBalance), Loans = count(distinct(tt.BaseLoanId))  
, RateRemainsSame = sum(case when tt.totalRate=tt.Previous_TotalRate then 1 else 0 end)
from (
select t.yr_month
, t.RateType
, Previous_RateType = LAG(t.RateType,1) OVER ( PARTITION BY t.BaseLoanId ORDER BY t.DATE)  
, SOMBalance = t.Balance 
, BaseLoanId = t.BaseLoanId
, TotalRate  = t.BaseRate
, Previous_TotalRate = LAG(t.BaseRate,1) OVER ( PARTITION BY t.BaseLoanId ORDER BY t.DATE)
from
(
select yr_month = concat(year(Date),month(date)),BaseLoanId = cast(substring(loanid,15,12) as float),Date
, BaseRate = round(BaseRate,2)
,RateType=
case 
 When INTRATETYPE in  ('BW VAR CON RIC CAPIT','MCQ RATA FIX DUR VAR','MCQ VAR CON RIC CAP.','VAR CON RIC CAPITALE','RATA FIX - DURATAVAR','VAR CON RIC CAP CERT','TASSO VARIABILE') THEN 'Variable'
 When INTRATETYPE in  ('FISSO 10 ANNI (RIN)' ,'TASSO FISSO','FISSO 5 ANNI') THEN 'Fixed'
 WHEN INTRATETYPE in  ('CONGUAGLIO POSTICIP.') THEN 'CHF'
 ELSE 'N/A' 
END
, Balance          = Balance + Arrears
from dynamic d
join (select * from live_option
where option_flag = 'Y') d2
on d2.Loan_Id = cast(substring(loanid,15,12) as float) and d.PRODUCTID = 6 and cast(substring(d.loanid,28,3) as float) in (0,1) ) t
) tt
group by tt.yr_month,
tt.RateType,tt.Previous_RateType



select * from dynamic
where BASELOANID = 323206
order by date