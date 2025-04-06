# Data-Engineering-2-Project
A Scalable Data Pipeline using GCP to perform sentiment analysis of Reddit Data


# Smartphone Market Sentiment Analysis

## Project Overview
This repository contains the code and resources for a data pipeline project aimed at analyzing and comparing user engagement for smartphones, particularly focusing on the iPhone 15 and its competitors. Utilizing data from Reddit, this pipeline captures, preprocesses, and analyzes sentiments to provide actionable insights.

## Pipeline Architecture
The data pipeline consists of the following components:
1. **Data Fetching**: Utilizing Reddit API to gather data.
2. **Data Ingestion**: Ingesting data using Google Cloud Pub/Sub and storing in Google Cloud Storage.
3. **Data Preprocessing**: Cleaning and structuring data using Python.
4. **Sentiment Analysis**: Analyzing sentiments using Pre-trained Model.
5. **Data Storage**: Storing processed data at each stage in Google Cloud Storage.
6. **Data Visualization**: Using Looker Studio for presenting the analysis results.

## Technologies Used
- Google Cloud Platform (GCP)
- Google Cloud Pub/Sub
- Google Cloud Storage
- BigQuery
- Python
- facebook/bart-large-mnli Pre-trained Model
- Looker Studio
---
## System Architecture 
<div align="center">
  <img width="768" alt="DE-2-Arch" src="https://github.com/user-attachments/assets/8b0dd6fe-60bd-40bb-b870-6cc48910be30" />
</div>

This project offers a scalable and efficient solution for sentiment analysis in the smartphone market, leveraging the power of modern data engineering and analytics tools.


