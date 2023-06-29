# MultiRelational_Database
A database model for a fictional retail chain of stores 

[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)
<h3 align="left">Languages and Tools:</h3>
<p align="left">
<a href="https://git-scm.com/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/git-scm/git-scm-icon.svg" alt="git" width="40" height="40"/>
<img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/postgresql/postgresql-original-wordmark.svg" alt="postgresql" width="40" height="40"/> </a> <a href="https://www.python.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> </a> <a href="https://www.selenium.dev" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/detain/svg-logos/780f25886640cef088af994181646db2f6b1a3f8/svg/selenium-logo.svg" alt="selenium" width="40" height="40"/> </a> </p>

# Running The Program


# Introduction and Case Study 
![Clear Pivot Image](https://github.com/WayneRose-95/MultiRelational_Database/assets/89411656/8906b516-deb4-40ec-83f4-24fea40eabd4)

Clear Pivot is a fictional chain of retail stores. 

Operating in three countries, USA, UK and Germany, the retail chain specialises in selling a generalised set of products. 

From DIY and homeware to toys, health and beauty products. 

The business has its data hosted in various data locations

- An Amazon RDS, which contains data on their users, stores and a list of their orders. 
- An Amazon S3 Bucket, which contains data on their products in .csv files and date_time_series data stored within .json files 
- A PDF file which contains details on credit card details hosted on AWS. 

The business needs a way to centralise these sources of data into a single source of truth; a centralised datastore which contains allows end users to be able to SQL run queries across these multiple datasources to provide insights into their data. 

## The Solution 
The proposal is to create a data warehouse using a STAR schema, by utilising the Kimbal methodology for creating data models. 

The raw data from the source systems will be:

- Extracted from their source systems 
- Transformed using the Pandas Package in Python  
- Loaded into a Postgresql database

From which after the ETL process has completed, relationships amongst tables will be formed to create the STAR schema design. 

## The Data Model 
Below is a design of the STAR schema for Clear Pivot
![image](https://github.com/WayneRose-95/MultiRelational_Database/assets/89411656/af09fa64-4b4d-4bef-96c9-8d076272d2fe)

