Data Modeling: Relational Postgres DB

The goal of this effort is to extract, transform and load data stored in JSON into a postgres database (DB).

The files include two notebook files (etl.ipynp and test.ipynp) used to test pieces of code and to validate the data loaded into the DB by table.

The create_table.py file contains code that creates the DB and the tables

The etl.py file extracts the data from JSON files, stores in in Dataframes, can calls SQL queries to update the DB tables 

The sql_queries.py contains all of the SQL queries called to create, and load the DB tables
 
Create_table.py should be run first to create the DB and tables, then the etl.py file should be run to load the data

