# Big-Data---Instagram-by-location

This repository contains the coded artifacts for the final project for COMP-548DL - Big Data Management and Processing of the University of Nicosia.

The project's purpose is to analyse data from Instagram posts and provide insights on the popularity of different locations on different time periods. The dataset contains information on 42M Posts, 1.2M Locations and 4.5M Profiles from Instagram from 2010 to 2019.

A document store database was selected (Firestore), based on the fact that Instagram posts would have more read operations than write operations (a post is viewed multiple times but is created only once). If the data are to be processed a lot of times, or on a regular basis, a better idea would be to join the collections and add some more information on a location or a user on each post's document. However, joining these 3 collections with millions of documents each would be very intensive and costly. So, after processing the posts, I only then joined the results with some fields from the other collections.

The procedured I followed is divided into the 3 notebooks of the repository as follows:

1) Load_data
  
  The first notebook is responsible for loading the data, and creating the files needed for the project. The notebook explains how the data was loaded to Firestore and how to create the files needed for the processing part and the streaming demo.

2) Historical_Data_Processing

  This notebook shows the processing that was made on Historical Data that we have stored. Since the volume of the data was very high, this notebook was ran on a Dataproc Cluster that consisted of 1 master node and 5 worker nodes (E2 -4CPU). (Google Cloud Platform)
  Processing was made using PySpark RDDs through some MapReduce operations and the SparkSQL DataFrame API for the sake of comparison. 

3) Streaming_Data_Demo
  
  The third notebook contains the streaming data simulation. The last 30 days of the dataset were taken, split into daily batches and fed into a stream processing pipeline using SparkStreaming. Some simple aggregation statistics were calculated after each trigger, and a Results table was constantly being updated.
  
To visualize the results I prepared a report with some Map Charts on Google Looker at:  
https://datastudio.google.com/reporting/e647d5ac-e2e2-437f-ac48-cb63d82fe382/page/p_zbw36zck2c
