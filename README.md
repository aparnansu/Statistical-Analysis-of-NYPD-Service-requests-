# Statistical-Analysis-of-NYPD-Service-requests
Service request data available at https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-
nwe9/data

1)complaint-analytics.py:
A python program to analyze the number of Illegal Parking, Noise and Street condition complaints recorded from 2017 to 2019.
Hadoop map reduce computation has been used for deriving the necessary results.

2)NYPD_Service_request_analysis.ipynb:
a)Data cleaning using Pyspark 
b)Implemented Random Forest regression on training set to create a model
c)Prediction of resolution times of service requests[Time difference between status change from "Open" to "Closed"]
