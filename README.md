# NOAA-weather-data-analysis

This project repository contains files for performing exploratory data analysis on the NOAA weather data https://www.ncdc.noaa.gov

This repository contains:
* Data Ingestion : dataingestion.py to programmatically download and store the data on S3
* Exploratory data analysis (preliminary) : plot distributions and detect outliers/bad data
* Data Wrangling : programmatically download data from S3 buckets and perform preprocessing and wrangling to clean the data
* Exploratory data analysis (on clean data): find patterns and insights using statistical and visualization tools
* Data-pipeline : Luigi pipeline to automate the extraction, transformation and loading of the data
* Docker : Build docker image to execute 

### Tools used:
Jupyter notebook, powerBI/Tableau 

### Programming Language:
Python
