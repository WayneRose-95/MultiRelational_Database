# MultiRelational_Database
A database model for a fictional retail chain of stores 

# Project Status 
In Development 
Current Feature: README and Wiki Documentation Last Updated 01/07/2023 (DD/MM/YYYY) 

<h1 align="left">Languages and Tools</h1>

[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

<p align="left">
<a href="https://git-scm.com/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/git-scm/git-scm-icon.svg" alt="git" width="40" height="40"/>
<img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/postgresql/postgresql-original-wordmark.svg" alt="postgresql" width="40" height="40"/> </a> <a href="https://www.python.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> </a> <a href="https://www.selenium.dev" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/detain/svg-logos/780f25886640cef088af994181646db2f6b1a3f8/svg/selenium-logo.svg" alt="selenium" width="40" height="40"/> </a> </p>

# Running The Program
To run this program, please follow this link for detailed instructions 

[Installation and Setup](https://github.com/WayneRose-95/MultiRelational_Database/wiki/Installation-Setup-and-Instructions)

# Introduction and Case Study 
![Clear Pivot Image](https://github.com/WayneRose-95/MultiRelational_Database/assets/89411656/8906b516-deb4-40ec-83f4-24fea40eabd4)

Clear Pivot is a fictional chain of retail stores. 

Operating in three countries, USA, UK and Germany, the retail chain specialises in selling a generalised set of products. 

From DIY and homeware to toys, health and beauty products. 

The business has its data hosted in various data locations

- An Amazon RDS, which contains data on their users, stores and a list of their orders. 
- An Amazon S3 Bucket, which contains data on their products in .csv files and date_time_series data stored within .json files 
- A PDF file which contains credit card details hosted on AWS. 

The business needs a way to centralise these sources of data into a single source of truth; a centralised datastore which contains allows end users to be able to SQL run queries across these multiple datasources to provide insights into their data. 

## The Solution 
The proposal is to create a data warehouse using a STAR schema, by utilising the Kimbal methodology for creating data models. 

The raw data from the source systems will be:

- Extracted from their source systems 
- Transformed using the Pandas Package in Python  
- Loaded into a Postgresql database

From which after the ETL process has completed, relationships amongst tables will be formed to create the STAR schema design. 

### The ETL Process
![image](https://github.com/WayneRose-95/MultiRelational_Database/assets/89411656/ec88c4c0-f9e4-492a-8ce4-728d2e17757e)



## The Data Model 
Below is a design of the STAR schema for Clear Pivot
![image](https://github.com/WayneRose-95/MultiRelational_Database/assets/89411656/1809e536-bdfc-4a34-a734-63bbc4cff305)

# Python Script Usage
The project consists of three main scripts which are responsible for the ETL process. 
These are listed below
![image](https://github.com/WayneRose-95/MultiRelational_Database/assets/89411656/9e9753b6-a4a7-4bae-9060-89c74ce23a42)


The functionality of each of these scripts is called via 

````````````````````````````````````
python main.py 
`````````````````````````````````````
# Business Questions 
After creating the STAR schema model Quick Pivot's Operations department wants to gain answers to the following questions: 

- How many stores does the business have an in which countries? 
- Which locations currently have the most stores? 
- Which months produce the average highest cost of sales typically? 
- What percentage of sales come through each type of store? 
- Which month in in each produced the highest cost of sales? 
- What is our staff headcount? 
- Which German store type is selling the most? 
- How quickly is the company making sales?

These questions are answered in the **sql_scripts.sql** script within this repository

# Future Implementations and Improvements 

- Automation of SQL Sripts to set datatypes in tables, and join dims and facts together
- Addition of extra rows in dimension tables to account for unknown and empty records
- Implementation of loggging for class modules
- Creation of unittests scripts for class modules 
- Create a backups directory containing the databases
- Create a currency table
- Create a currency conversion table
- Adjust the order of the ETL process
- Create LAND tables for dimension table
- Add CTRL tables for ETL process
- Addition of slowly changing dimensions (SCDs)




