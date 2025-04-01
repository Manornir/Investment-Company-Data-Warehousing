--====================================================================START
use tar2_grs
--======================================================================END


--====================================================================START
--CREATE TABLE FOR APPROVED EMAIL SUPPLIERS
CREATE TABLE [meta.Approved_Email_suppliers] (
	Email NVARCHAR(50) UNIQUE
	);
--======================================================================END


--====================================================================START
--INSERT ALL APPROVED EMAILS SUPPLIERS
INSERT INTO [meta.Approved_Email_suppliers](Email)
SELECT DISTINCT(RIGHT(LEFT(email, CHARINDEX('.',email)-1),CHARINDEX('@',REVERSE(LEFT(email, CHARINDEX('.',email)-1)))-1)) AS email_supplier
FROM [grs.past_investors] pi
--======================================================================END


--====================================================================START
--INSERT ANOTHER APPROVED EMAIL SUPPLIER - 'grazi' IS WAS NOT AN APPROVED SUPPLIER BEFORE
insert into [meta.Approved_Email_suppliers](email)
VALUES('grazi')
--======================================================================END


--====================================================================START
--COPY [grs.stock_spots] TO [meta.stock_spots] USING SELECT INTO
--INSERT TABLE [grs.stock_spots] TO [meta.stock_spots]
SELECT IDENTITY(SMALLINT,5000,1) AS stockspot_id, *
INTO [meta.stock_spots]
FROM [grs.stock_spots]
--======================================================================END


--====================================================================START
--RENAMING COLUMNS OF meta.stock_spots
EXEC sp_rename '[meta.stock_spots].num', 'stock_id', 'COLUMN'
EXEC sp_rename '[meta.stock_spots].time', 'date', 'COLUMN'
--======================================================================END


--====================================================================START
--COPY [grs.exchangerates] TO [meta.exchangerates] USING SELECT INTO
--INSERT TABLE [grs.exchangerates] TO [meta.exchangerates]
SELECT IDENTITY(SMALLINT,9000,1) AS exrate_id, *
INTO [meta.exchangerates]
FROM [grs.exchangerates]
--======================================================================END


--====================================================================START
--CREATE A TABLE OF meta.state
CREATE TABLE [meta.state] (
	state_id SMALLINT IDENTITY(1,1) PRIMARY KEY,
	state_name NVARCHAR(50),
	cur NVARCHAR(50),
	phone_code NVARCHAR(50)
	);
--======================================================================END


--====================================================================START
INSERT INTO [meta.state](state_name, cur, phone_code)
SELECT DISTINCT(i.state), i.cur, 
				CASE --phone code
				WHEN i.state = 'Israel' THEN '972'
				WHEN i.state = 'France' THEN '33'
				WHEN i.state = 'Spain' THEN '34'
				WHEN i.state = 'Germany' THEN '49'
				WHEN i.state = 'Greece' THEN '30'
				WHEN i.state = 'USA' THEN '1'
				WHEN i.state = 'England' THEN '44'
				ELSE i.state
				END
FROM [grs.past_investors] i --since all states in newinvestors are also in past_investors except France, i chose to use newinvestors table only
--======================================================================END


--====================================================================START
--CREATE A TABLE OF DIM INVESTOR
CREATE TABLE [tbl.dim_investor] (
	inv_id SMALLINT IDENTITY(2001,1) PRIMARY KEY,
	first_name NVARCHAR(50),
	last_name NVARCHAR(50),
	email NVARCHAR(50),
	state_id SMALLINT,
	Phone NVARCHAR(50),
	annual_salary INT,
	curr_status NVARCHAR(50)
	);
--======================================================================END


--====================================================================START
--INSERT TABLE OF PAST INVESTORS TO TABLE OF dim investors (FULL NAME IS SPLITED)
INSERT INTO [tbl.dim_investor](first_name, last_name, email, curr_status, state_id, Phone, annual_salary)
SELECT	CASE --first_name
		WHEN 0 < CHARINDEX(' ',i.name, 1) -- there are spaces
		THEN LEFT(i.name, CHARINDEX(' ',i.name, 1)-1)
		ELSE i.name
		END,
		CASE --last_name
		WHEN 0 < CHARINDEX(' ',i.name, 1) -- there are spaces
		THEN SUBSTRING(i.name, CHARINDEX(' ',i.name, 1)+1,LEN(i.name) - CHARINDEX(' ',i.name, 1))
		ELSE NULL --there is only first name
		END,
		i.email,
		--curr_status
		'past',
		--state_id
		(SELECT j.state_id FROM [meta.state] j WHERE i.state = j.state_name),
		i.phone, i.annual_salary
FROM [grs.past_investors] i
--======================================================================END


--====================================================================START
--INSERT TABLE OF NEW INVESTORS TO TABLE OF dim investor (FULL NAME IS SPLITED)
INSERT INTO [tbl.dim_investor](first_name, last_name, email, curr_status, state_id, Phone, annual_salary)
SELECT CASE --first_name
		WHEN 0 < CHARINDEX(' ',i.Investor_Name, 1) -- there are spaces
		THEN LEFT(i.Investor_Name, CHARINDEX(' ',i.Investor_Name, 1)-1)
		ELSE i.Investor_Name
		END,
		CASE --last_name
		WHEN 0 < CHARINDEX(' ',Investor_Name, 1) -- there are spaces
		THEN SUBSTRING(Investor_Name, CHARINDEX(' ',i.Investor_Name, 1)+1,LEN(i.Investor_Name) - CHARINDEX(' ',i.Investor_Name, 1))
		ELSE NULL --there is only first name
		END,
		i.Email,
		--curr_status
		'new',
		--state_id
		CASE
		WHEN i.state='Israeli' THEN (SELECT j.state_id FROM [meta.state] j WHERE j.state_name = 'Israel')
		ELSE (SELECT j.state_id FROM [meta.state] j WHERE i.state = j.state_name)
		END,
		i.Phone, i.income 
FROM [grs.newinvestors] i
--======================================================================END


--====================================================================START
--NORMALIZE dim investors
--NORMALIZE Phone TO UNIFORM FORMAT
UPDATE
    [tbl.dim_investor]
SET
	Phone = 
				CASE
				WHEN i.curr_status = 'new' AND i.state_id = (SELECT i.state_id WHERE j.state_name = 'Israel')
				THEN (SELECT CONCAT(j.phone_code+'-',CONCAT(SUBSTRING(Phone,2,2),RIGHT(Phone,LEN(Phone)-4))))
				WHEN i.curr_status = 'new' AND i.state_id IN (SELECT i.state_id WHERE j.state_name != 'Israel')
				THEN (SELECT CONCAT(j.phone_code+'-',Phone))
				ELSE Phone
				END
FROM [tbl.dim_investor] i inner join [meta.state] j on i.state_id = j.state_id WHERE i.inv_id = inv_id
--======================================================================END


--====================================================================START
--CREATING A TRIGGER THAT CHECKS WHETHER A SUPPLIER, IN AN ADDED EMAIL ADDRESS, IS APPROVED
CREATE TRIGGER supplier_check ON [tbl.dim_investor]
AFTER INSERT
AS
begin
DECLARE @supplier NVARCHAR(50);
DECLARE @email NVARCHAR(50);

SELECT @email = inserted.Email
FROM inserted;

--extract email supplier from an email address
SET @supplier = (SELECT DISTINCT(RIGHT(LEFT(@email, CHARINDEX('.',@email)-1),CHARINDEX('@',REVERSE(LEFT(@email, CHARINDEX('.',@email)-1)))-1)) AS new_supplier
				FROM inserted)

SELECT @email = inserted.Email
FROM inserted;
	IF @supplier NOT IN (
	SELECT *
	FROM [meta.Approved_Email_suppliers]
	)
	BEGIN
		RAISERROR('Supplier is not in approved. It has to be in [meta.Approved_Email_suppliers] table',16,1)
		ROLLBACK TRANSACTION
	END
END;
--======================================================================END


--====================================================================START
CREATE TRIGGER check_phone_code ON [tbl.dim_investor]
AFTER INSERT
AS
begin
--inserted variables in [tbl.dim_investor] table
DECLARE @ins_state_id SMALLINT
DECLARE @ins_phone_code SMALLINT

SELECT @ins_state_id=inserted.state_id
FROM inserted;

SELECT @ins_phone_code = LEFT(inserted.Phone,CHARINDEX('-',inserted.Phone)-1)
FROM inserted

--check if inserted phone code matches the correct phone code by its corresponding state
IF @ins_phone_code NOT IN (SELECT st.phone_code FROM [meta.state] st WHERE st.state_id=@ins_state_id)
BEGIN
		RAISERROR('Phone code does not match the state_id you have entered',16,1)
		ROLLBACK TRANSACTION
	END
END;
--======================================================================END


--====================================================================START
--CREATE A DIM TABLE OF dim_broker
CREATE TABLE [tbl.dim_broker] (
	broker_id SMALLINT identity(1001,1) PRIMARY KEY,
	first_name NVARCHAR(50),
	last_name NVARCHAR(50),
	bdate DATE
	);
--======================================================================END


--====================================================================START
--INSERT AND NORMALIZE ALL VALUES OF BROKERS
INSERT INTO [tbl.dim_broker](first_name, last_name, bdate)
SELECT	CASE --first_name
		WHEN 0 < CHARINDEX(' ',name, 1) -- there are spaces
		THEN LEFT(name, CHARINDEX(' ',name, 1)-1)
		ELSE name
		END,
		CASE --last_name
		WHEN 0 < CHARINDEX(' ',name, 1) -- there are spaces
		THEN SUBSTRING(name, CHARINDEX(' ',name, 1)+1,LEN(name) - CHARINDEX(' ',name, 1))
		ELSE NULL --there is only first name
		END,
		bdate
FROM [grs.brokers]
--======================================================================END


--====================================================================START
--CREATE A DIM TABLE OF dim_sales_team
CREATE TABLE [tbl.dim_sales_team] (
	team_id SMALLINT PRIMARY KEY, --team_id=managerid
	);
--======================================================================END


--====================================================================START
--INSERT managerid TO BE team_id
INSERT INTO [tbl.dim_sales_team](team_id)
SELECT DISTINCT(managerid)
FROM [grs.brokers]
--======================================================================END


--====================================================================START
--CREATE A FACT TABLE OF fact_broker-in-team
CREATE TABLE [tbl.fact_broker-in-team] (
	team_id SMALLINT NOT NULL, --team_id=managerid
	broker_id SMALLINT NOT NULL, --broker_id=num
	FOREIGN KEY(team_id) REFERENCES [tbl.dim_sales_team](team_id) ON DELETE CASCADE,
	FOREIGN KEY(broker_id) REFERENCES [tbl.dim_broker](broker_id) ON DELETE CASCADE
	);
--======================================================================END


--====================================================================START
--INSERT managerid TO BE team_id
INSERT INTO [tbl.fact_broker-in-team](team_id, broker_id)
SELECT managerid, num --managerid=team_id, broker_id=num
FROM [grs.brokers]
--======================================================================END


--====================================================================START
--CREATE A TABLE OF dim stock
CREATE TABLE [tbl.dim_stock] (
	stock_id SMALLINT identity(3001,1) PRIMARY KEY,
	stock_name NVARCHAR(50),
	stock_type NVARCHAR(50)
	);
--======================================================================END


--====================================================================START
--INSERT TABLE OF STOCKS TO TABLE OF dim_stock
INSERT INTO [tbl.dim_stock](stock_name, stock_type)
SELECT name, type
FROM [grs.stocks]
--======================================================================END

--CREATE A FACT TABLE OF call-broker-investor-stock (TASK 1):
--====================================================================START
--CREATING A DIM TABLE OF [tbl.dim_call] USING SELECT INTO:

--COPY THE COLUMNS "date, value" FROM CALLS_TRADES_IID TABLE TO tbl.dim_call TABLE
SELECT IDENTITY(SMALLINT, 4001, 1) AS call_id, date, stock, value
INTO [tbl.dim_call]
FROM CALLS_TRADES_IID cti
--======================================================================END


--====================================================================START
--ADD COLUMNS call_status, call_status, comments TO tbl.dim_call
ALTER TABLE [tbl.dim_call] 
ADD CONSTRAINT call_id PRIMARY KEY(call_id), call_status NVARCHAR(50), cost FLOAT, comments NVARCHAR(50);
--======================================================================END


--====================================================================START
--NORMALIZE TABLE tbl.dim_call BY UPDATING NEW COLUMNS (VALUES ARE MOVED FROM OLD COLUMNS BY SOME CONDITIONS)
UPDATE
    [tbl.dim_call]
SET
			call_status =
						CASE --fill call_status column with 'purchased' or 'sold' based on data from CALLS_TRADES_IID table
		
						WHEN '0' = c.stock --the value in stock column is 0
						THEN NULL
		
						WHEN '-' = LEFT(c.value,1) --the price/shares are negative => 'sold'
						THEN 'sold'
		
						WHEN '-' != LEFT(c.value,1) --the price/shares are positive => 'purchased'		
						THEN 'purchased'
		
						ELSE NULL --the value is NULL => NULL
						END,
			cost =
					CASE
					
					--if there is a dollar sign and value is positive - fill cost with the number:
					WHEN '$' = (SELECT RIGHT(c.value,1)) AND '-' != (SELECT LEFT(c.value,1))
					THEN (SELECT CAST(CONVERT(numeric(38, 12),LEFT(c.value,len(c.value)-1)) AS FLOAT))
					
					--if there is a dollar sign and value is negative - fill value with the number without minus:
					WHEN '$' = (SELECT RIGHT(c.value,1)) AND '-' = (SELECT LEFT(c.value,1))
					THEN (SELECT CAST(CONVERT(NUMERIC(38, 12), SUBSTRING(c.value, 2, LEN(c.value)-2)) AS FLOAT))
					
					--if there is no dollar sign - calculate the value: (num_of_shares * value of one share):
					WHEN '$' != (SELECT RIGHT(c.value,1)) AND '-' != (SELECT LEFT(c.value,1))
					THEN (SELECT CAST(CONVERT(NUMERIC(38, 12),c.value) * ss.value AS FLOAT))
					
					WHEN '$' != (SELECT RIGHT(c.value,1)) AND '-' = (SELECT LEFT(c.value,1))
					THEN (SELECT CAST(CONVERT(NUMERIC(38, 12),REPLACE(c.value,'-','')) * ss.value AS FLOAT))
					
					ELSE NULL --no deal = value is null
					END,
			comments =
						CASE
						--value in stock column is a comment = neither a number nor a stock name
						WHEN 1 != ISNUMERIC(c.stock) AND c.stock NOT IN (SELECT DISTINCT(st.stock_name) from [tbl.dim_stock] st)
						THEN c.stock
						
						ELSE NULL
						END
FROM [tbl.dim_call] c LEFT OUTER JOIN [tbl.dim_stock] st
ON (CASE WHEN isnumeric(c.stock) = 1 AND c.stock in (st.stock_id) THEN st.stock_id
		 WHEN c.stock = st.stock_name THEN st.stock_id
		 ELSE NULL
		 END) = st.stock_id
		 LEFT OUTER JOIN [meta.stock_spots] ss ON c.date = ss.date AND st.stock_id=ss.stock_id
--======================================================================END


--====================================================================START
--REMOVE OLD COLUMNS STOCK AND VALUE COLUMNS
ALTER TABLE [tbl.dim_call]
DROP COLUMN stock, value;
--THE FINAL NORMALIZED TABLE IS CREATED HERE
--======================================================================END


--====================================================================START
--CREATING A FACT TABLE OF [tbl.fact_call-broker-investor-stock] USING SELECT INTO:

--COPY THE COLUMNS "broker, stock, iid" FROM CALLS_TRADES_IID TABLE TO tbl.fact_call-broker-investor-stock TABLE
SELECT IDENTITY(SMALLINT, 4001, 1) AS call_id, broker, stock, iid
INTO [tbl.fact_call-broker-investor-stock]
FROM CALLS_TRADES_IID
--======================================================================END


--====================================================================START
--ADD COLUMNS inv_id, broker_id, stock_id TO tbl.fact_call-broker-investor-stock AND SET FOREIGN KEYS
ALTER TABLE [tbl.fact_call-broker-investor-stock]
ADD inv_id SMALLINT, broker_id SMALLINT, stock_id SMALLINT
FOREIGN KEY (call_id) REFERENCES [tbl.dim_call](call_id),
FOREIGN KEY (inv_id) REFERENCES [tbl.dim_investor](inv_id),
FOREIGN KEY (broker_id) REFERENCES [tbl.dim_broker](broker_id),
FOREIGN KEY (stock_id) REFERENCES [tbl.dim_stock](stock_id);
--======================================================================END


--====================================================================START
--NORMALIZE TABLE tbl.fact_call-broker-investor-stock BY UPDATING NEW COLUMNS (VALUES ARE MOVED FROM OLD COLUMNS BY SOME CONDITIONS)
UPDATE
    [tbl.fact_call-broker-investor-stock]
SET
			inv_id =
						fcbis.iid,
			broker_id =
						CASE
						WHEN 1 != ISNUMERIC(fcbis.broker) --the value is not an id
						THEN (
								SELECT CAST(b_fname.broker_id AS SMALLINT)
								FROM (
										(SELECT fcbis.broker) fcbis_fname 
										INNER JOIN 
										(SELECT br.broker_id ,first_name + ' ' + COALESCE(last_name, '') AS name 
											FROM [tbl.dim_broker] br) b_fname
										ON fcbis_fname.broker=b_fname.name)
								)
						ELSE CAST(broker AS SMALLINT)
						END,
			stock_id =
						CASE
						WHEN '0' = fcbis.stock OR (1 != ISNUMERIC(fcbis.stock) AND fcbis.stock NOT IN (SELECT DISTINCT(st.stock_name) from [tbl.dim_stock] st)) --the value is either 0 or a comment
						THEN NULL
						WHEN fcbis.stock IN (SELECT DISTINCT(st.stock_name)) --the value is stock name
						THEN (
						SELECT CAST(st.stock_id AS SMALLINT)
						WHERE fcbis.stock=st.stock_name
						)
						ELSE CAST(fcbis.stock AS SMALLINT) --the value is a stock id
						END
FROM [tbl.fact_call-broker-investor-stock] fcbis LEFT OUTER JOIN [tbl.dim_stock] st
ON (CASE WHEN isnumeric(fcbis.stock) = 1 AND fcbis.stock in (st.stock_id) THEN st.stock_id
		 WHEN fcbis.stock = st.stock_name THEN st.stock_id
		 ELSE NULL
		 END) = st.stock_id
--======================================================================END


--====================================================================START
--REMOVE OLD COLUMNS BROKER, IID AND STOCK COLUMNS
ALTER TABLE [tbl.fact_call-broker-investor-stock]
DROP COLUMN broker, iid, stock;
--THE FINAL NORMALIZED TABLE IS CREATED HERE
--======================================================================END


--CREATE DATA MARTS:
--CREATE VIEWS FOR SALES TEAM MANAGERS (TASK 2):
--====================================================================START
--new investors - data for sales team managers
CREATE VIEW [mart.new_invs_team managers] as (
	SELECT *
	FROM [tbl.dim_investor]
	WHERE curr_status = 'new'
);
--====================================================================END


--====================================================================START
--brokers revenues per month (ranked) - data for sales team managers
CREATE VIEW [mart.broker_sales_per_month_ranked] as (
	SELECT fbis.broker_id, MONTH(c.date) AS month, CAST(sum(c.cost / ss.value) AS SMALLINT) as num_of_shares, sum(c.cost) as revenue,
	DENSE_RANK() over (order by sum(c.cost) DESC) d_rank_num
	FROM [tbl.dim_call] c LEFT OUTER JOIN [tbl.fact_call-broker-investor-stock] fbis ON c.call_id=fbis.call_id 
		 LEFT OUTER JOIN [meta.stock_spots] ss ON fbis.stock_id=ss.stock_id AND c.date=ss.date
	GROUP BY fbis.broker_id, MONTH(c.date)
);
--====================================================================END


--====================================================================START
--broker ranking by revenue - data for sales team managers
CREATE VIEW [mart.brokers_ranking_by_revenue] as (
	SELECT fbis.broker_id, CAST(sum(c.cost / ss.value) AS INT) as num_of_shares, sum(c.cost) as revenue,
		DENSE_RANK() over (order by sum(c.cost) DESC) d_rank_num
	FROM [tbl.dim_call] c LEFT OUTER JOIN [tbl.fact_call-broker-investor-stock] fbis ON c.call_id=fbis.call_id
		 LEFT OUTER JOIN [meta.stock_spots] ss ON fbis.stock_id=ss.stock_id
	GROUP BY fbis.broker_id
);
--====================================================================END

--CREATE VIEWS FOR THE CEO (TASK 3):
--====================================================================START
--revenue made by teams per day - data for CEO (ranked)
CREATE VIEW [mart.teams_sales_per_day] as (
	SELECT fbit.team_id, c.date, CAST(sum(c.cost / ss.value) AS SMALLINT) as num_of_shares, sum(c.cost) as revenue,
	DENSE_RANK() over (order by sum(c.cost) DESC) d_rank_num
	FROM [tbl.dim_call] c LEFT OUTER JOIN [tbl.fact_call-broker-investor-stock] fbis ON c.call_id=fbis.call_id
		 LEFT OUTER JOIN [meta.stock_spots] ss ON fbis.stock_id=ss.stock_id AND c.date=ss.date
		 LEFT OUTER JOIN [tbl.fact_broker-in-team] fbit ON fbis.broker_id=fbit.broker_id
	GROUP BY fbit.team_id, c.date
);
--====================================================================START
--revenue made by teams per month - data for CEO (ranked)
CREATE VIEW [mart.teams_sales_per_month] as (
	SELECT fbit.team_id, MONTH(c.date) AS month, CAST(SUM(c.cost / ss.value) AS SMALLINT) as num_of_shares, SUM(c.cost) as revenue,
	DENSE_RANK() over (order by SUM(c.cost) DESC) d_rank_num
	FROM [tbl.dim_call] c LEFT OUTER JOIN [tbl.fact_call-broker-investor-stock] fbis ON c.call_id=fbis.call_id
		 LEFT OUTER JOIN [meta.stock_spots] ss ON fbis.stock_id=ss.stock_id AND c.date=ss.date
		 LEFT OUTER JOIN [tbl.fact_broker-in-team] fbit ON fbis.broker_id=fbit.broker_id
	GROUP BY fbit.team_id, MONTH(c.date) 
);
--====================================================================END


--====================================================================START
--team ranking by revenue - data for CEO
CREATE VIEW [mart.team_ranking_by_revenue] as (
	SELECT fbit.team_id, CAST(sum(c.cost / ss.value) AS INT) as num_of_shares, sum(c.cost) as revenue,
		DENSE_RANK() over (order by sum(c.cost) DESC) d_rank_num
	FROM [tbl.dim_call] c LEFT OUTER JOIN [tbl.fact_call-broker-investor-stock] fbis ON c.call_id=fbis.call_id
		 LEFT OUTER JOIN [meta.stock_spots] ss ON fbis.stock_id=ss.stock_id AND c.date=ss.date
		 LEFT OUTER JOIN [tbl.fact_broker-in-team] fbit ON fbis.broker_id=fbit.broker_id
	GROUP BY fbit.team_id
);

--====================================================================END


--CREATE VIEW FOR BOOKKEEPING (TASK 4):
--====================================================================START
--calculate the salary of each employee every month - data for bookkeeping

CREATE VIEW [mart.brokers_salaries] as 
	WITH salary_per_call as (SELECT fbis.broker_id, c.date, 
							 (100*COUNT(DISTINCT(c.date)) +
							 (CASE 
							 WHEN fbis.stock_id IS NOT NULL AND c.call_status IS NOT NULL AND c.cost IS NOT NULL
							 THEN SUM(c.cost) * (CASE WHEN s.stock_type = 'Blue Chip Stock' THEN 0.01 ELSE 0.5 END)
							 ELSE 0 --the value is null - no transaction is made
							 END)) AS salary
							 FROM [tbl.dim_call] c LEFT OUTER JOIN [tbl.fact_call-broker-investor-stock] fbis ON c.call_id=fbis.call_id
								  LEFT OUTER JOIN [tbl.dim_broker] br ON fbis.broker_id=br.broker_id
								  LEFT OUTER JOIN [tbl.dim_stock] s ON fbis.stock_id=s.stock_id
							 GROUP BY fbis.broker_id, fbis.stock_id, c.date, c.call_status, c.cost, s.stock_type),
		  salary_per_day as 
							(SELECT spc.broker_id, spc.date, SUM(spc.salary) as salary
							FROM salary_per_call spc
							GROUP BY spc.date, spc.broker_id)
	SELECT spd.broker_id, MONTH(spd.date) as month, SUM(spd.salary) as salary
	FROM salary_per_day spd
	GROUP BY spd.broker_id, MONTH(spd.date);
--====================================================================END


--CREATE VIEWS FOR CFO (TASK 5):
--====================================================================START
--calculate the company's revenue per month
CREATE VIEW [mart.c_monthly_revenues] as (
	SELECT MONTH(daily_revenues.date) AS month, SUM(daily_revenues.revenues) AS revenue --monthly revenues
	FROM ( --daily revenues
			SELECT c.date, SUM(
			c.cost * (CASE WHEN s.stock_type IS NOT NULL THEN (CASE WHEN s.stock_type = 'Blue Chip Stock' THEN 0.0025 ELSE 0.005 END) ELSE 0 END) +
					(CASE
					 WHEN stt.cur='Shekel'
					 THEN (CASE WHEN c.call_status='purchased' THEN CEILING(exr.s_to_d * 0.02) ELSE FLOOR(exr.s_to_d * 0.02) END)
															
					 WHEN stt.cur='Euro'
					 THEN (CASE WHEN c.call_status='purchased' THEN CEILING(exr.e_to_d * 0.01) ELSE FLOOR(exr.e_to_d * 0.01) END) 
															 
					 ELSE 0
					 END)) AS revenues
			FROM [tbl.dim_call] c LEFT OUTER JOIN [tbl.fact_call-broker-investor-stock] fbis ON c.call_id=fbis.call_id
				LEFT OUTER JOIN [tbl.dim_stock] s ON fbis.stock_id=s.stock_id
				LEFT OUTER JOIN [meta.exchangerates] exr ON c.date=exr.date
				LEFT OUTER JOIN [tbl.dim_investor] i ON fbis.inv_id=i.inv_id
				LEFT OUTER JOIN [meta.state] stt ON i.state_id=stt.state_id
			GROUP BY c.date
	) daily_revenues
	GROUP BY MONTH(daily_revenues.date)
);
--====================================================================END


--CALCULATE THE COMPANY'S GROSS PROFITS (AFTER SALARIES) FOR CFO (TASK 5):
--====================================================================START
--calculate the company's gross profits each month
CREATE VIEW [mart.monthly_gross_profits] as (
	SELECT mr.month ,(mr.revenue - xpns.total_salaries) AS profit
	FROM [mart.c_monthly_revenues] mr
	INNER JOIN (SELECT bs.month, SUM(bs.salary) AS total_salaries FROM [mart.brokers_salaries] bs GROUP BY bs.month) xpns -- total expenses each month
	ON mr.month=xpns.month
);
--====================================================================END


--====================================================================START
--calculate the company's total gross profits
SELECT SUM(mgp.profit) FROM [mart.monthly_gross_profits] mgp
--====================================================================END


--====================================================================START
--FUNCTION THAT RETURNS HIGHEST PAID BROKER GIVEN A MONTH
CREATE FUNCTION highest_paid (@month SMALLINT)
RETURNS SMALLINT
AS
BEGIN
DECLARE @broker_id SMALLINT
SELECT @broker_id=brs.broker_id
FROM [mart.brokers_salaries] brs
WHERE brs.salary = (SELECT MAX(brs1.salary) FROM [mart.brokers_salaries] brs1 WHERE brs1.month=@month)
RETURN @broker_id
END;
--====================================================================END
