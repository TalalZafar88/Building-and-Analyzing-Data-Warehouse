# Building-and-Analysing-Data-Warehouse

## Table of contents
* [Project Description]
* [Tools and Technologies]
* [Setup]

Project Description
The aim of the project was to design, implement, and analyze a Data Warehouse (DW) prototype so that we make analysis of shopping behavior, optimize selling techniques etc. We had to build the warehouse with the data source which we were given. For this we implemented a real time ETL (Extraction, Transformation, and Loading) because the warehouse schema is different from database. We implemented the **Mesh Join** algorithm for integrating the transactional data with master data before loading into warehouse. After building the warehouse I analyzed the DW by applying OLAP queries.

# Tools and Technologies
1-Java

2-SQL

3-Google Guava

# Setup
The project consists of following executable files :

a JAVA file which contains the implementation of Mesh Join
an sql file for creating the warehouse schema
an sql file for analysing data warehouse through OLAP queries


The Mesh Join algorithm has been implemented in Java on eclipse and requires the following packages to run successfully :

JDBC jar file to connect to mysql database
Google Guava jar file to implement the functionality of Multi hash-map in Java

When the Java file runs successfully it will load the data into the warehouse schema for analysis
