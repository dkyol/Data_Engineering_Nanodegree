This goal of this project is to migrate the data from a music streaming startup into a cloud data warehouse. This migration is can be expected of any growing company looking to improve the reliability and speed of their operations.  The data resides in a AWS S3 bucket in a JSON formatted file of log data.

The scope of this project is to build an ETL pipeline to extract the data from S3, stage it in Redshift, and transforms it data into a set of dimensional tables leveraging the star schema so that the data is easily queryable and will aloow for the analytics team to find insights to reduce cost and drive growth.  

As mentioned the star schema will be used to load the data.  The data will be transformed into that schema and staged in redshift where it can be queried using SQL statements.

For example the following SQL Command would yield a comprehensive list of all the different artists that have been streamed on the platform:

SELECT distinct (name) FROM artist_table;
