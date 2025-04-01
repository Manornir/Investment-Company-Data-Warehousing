# Investment-Company-Data-Warehousing
- This project was developed as the final assignment for my "Data Warehousing" course during my B.Sc. degree.
- The implementation was done using SQL Server.
# Workflow
- I started with raw data tables containing unorganized information about the company and its activities (refer to the "raw files" folder included in the repository).
- I established the data warehouse scheme (including Dimensions, Facts and Metadata tables) based on the company's operational data.
- I transformed the raw data into a tidied and normalized format suitable for loading into the data warehouse (ETL process).
- I implemented Functions, Triggers, Procedures, Views, and Data Marts to develop a functional warehouse capable of handling daily operations efficiently.
# How to Use
To establish the data warehouse in SQL Server, execute each code block in the file separately, proceeding downward from the top. A block begins with leading '-'s and '='s, followed by 'START', and ends with the same format, followed by 'END'.
# Data Warehouse Contents
### Data Tables:

1. tbl.dim_investor
2. tbl.dim_broker
3. tbl.dim_sales_team
4. tbl.fact_broker-in-team
5. tbl.dim_stock
6. tbl.dim_call
7. tbl.fact_call-broker-investor-stock
8. meta.Approved_Email_suppliers
9. meta.stock_spots
10. meta.exchangerates
11. meta.state
##### Marts For Sales Team Managers
13. mart.new_invs_team managers
14. mart.broker_sales_per_month_ranked
15. mart.brokers_ranking_by_revenue
##### Marts For CEO
17. mart.teams_sales_per_day
18. mart.teams_sales_per_month
19. mart.team_ranking_by_revenue
##### Mart For Bookkeeping
21. mart.brokers_salaries
##### Marts For CFO
23. mart.c_monthly_revenues
24. mart.monthly_gross_profits

In addition to the data tables listed above, the Data Warehouse also includes a variety of practical functions, triggers and views.
