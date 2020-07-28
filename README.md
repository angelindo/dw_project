Description: The ETL goal is to provide a set of dimensional tables on the cloud ready to analytics.

Files:
- create_tables.py: Python script which creates all tables necesary for the pipeline
- dwh.cfg: Configuration file with credentials
- etl.py: Main script which will fill the data on all tables
- sql_queries.py: Queries necesary to create tables and insert data on redshift  

How to run:
- Change if necesarry the credentials on dwh.cfg
- Then run create_tables.py
- Lastly, run etl.py

Tables distribution:

- fct_songplay and dim_user tables are the ones distribuited by key, because it is more probable to join these two tables in future queries and a userid could have a large history of songs listened, so it is better to the two be distributed together.
- All others tables are distributed in all slices for a better performance.