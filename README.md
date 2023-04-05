# Capstone Project
![dtc logo](images/dtc_logo_.png)

## Preamble/Thank you.

I would like to express our appreciation for the smart, talented people of Data Engineering Zoomcamp and its helpful community for putting the course that helped us gain the skills and knowledge necessary to build this pipeline. Their guidance and expertise have been invaluable in our journey towards building efficient and effective data pipelines.


## Introduction

Data pipelines are critical to modern data infrastructure, as they allow businesses to process, analyze, and store vast amounts of data efficiently. A common use case involves moving data from MongoDB to Google Cloud Storage (GCS) to BigQuery (BQ) for further analysis. At Dashlabs.ai, we use Argo as our primary orchestration tool. However, for this specific use case, I will use Prefect as a test bed to build and monitor the pipeline. 


### Problem statement

As a final project to conclude the Data Engineering Zoomcamp. We will try to create a data pipeline extracting data stored in MongoDB, I will use the `airbnb sample dataset`.

The pipeline involves moving data from MongoDB to GCS to BQ for further analysis, followed by visualizing and analyzing the data with Metabase. The Google Cloud Storage and BigQuery Python APIs allow us to load and transform the data, respectively, while Metabase connects to the BQ warehouse and enables us to explore and analyze their data. By leveraging these tools and technologies, we are able to optimize our data pipeline and gain valuable insights from our data.

![arch_diag](images/architecture_diagram.png)

## Setup Environment

I use conda with Miniforge for this project, since I'm using M1 Mac. Following these instructions

https://naolin.medium.com/conda-on-m1-mac-with-miniforge-bbc4e3924f2b

On your terminal, follow this.
- create a new environment `conda create -n capstone python=3.9.16`
- activate env_name `conda activate capstone`
- install the packages `pip install -r requirements.txt`

## Setup Pipeline 

Crash Course on MongoDB

The purpose of this is to generate the credentials  or URI string needed to access the MongoDB database. Your URI string should look something like this
`mongodb+srv://m001-student:*****@sandbox.7x9ridu.mongodb.net/admin`

![URI](images/mongodb_atlas.png)

![sample_airbnb](images/mongodb_atlas_sample_data.png)

   1. Get started with [MongoDB Atlas](https://learn.mongodb.com/learn/course/getting-started-with-mongodb-atlas/lesson-2-creating-and-deploying-at-atlas-cluster/learn?client=customer&page=1&wvideo=q744h1g773). 
   <p><a href="https://learn.mongodb.com/learn/course/getting-started-with-mongodb-atlas/lesson-2-creating-and-deploying-at-atlas-cluster/learn?client=customer&amp;page=1&amp;wvideo=q744h1g773"><img src="https://embed-ssl.wistia.com/deliveries/42f644803955636b8b57a75a3dc6da4f.jpg?image_play_button_size=2x&amp;image_crop_resized=960x540&amp;image_play_button=1&amp;image_play_button_color=00ed64e0" width="400" height="225" style="width: 400px; height: 225px;"></a></p><p><a href="https://learn.mongodb.com/learn/course/getting-started-with-mongodb-atlas/lesson-2-creating-and-deploying-at-atlas-cluster/learn?client=customer&amp;page=1&amp;wvideo=q744h1g773"></a></p>   

   2. Using [MongoDB Connection Strings](https://learn.mongodb.com/learn/course/connecting-to-a-mongodb-database/lesson-1-using-mongodb-connection-strings/learn?client=customer&wvideo=eh4048sbrf) 

<p><a href="https://learn.mongodb.com/learn/course/connecting-to-a-mongodb-database/lesson-1-using-mongodb-connection-strings/learn?client=customer&amp;wvideo=eh4048sbrf"><img src="https://embed-ssl.wistia.com/deliveries/92534a1a893a64cbba544ac9d1ec8c00.jpg?image_play_button_size=2x&amp;image_crop_resized=960x540&amp;image_play_button=1&amp;image_play_button_color=00ed64e0" width="400" height="225" style="width: 400px; height: 225px;"></a></p><p><a href="https://learn.mongodb.com/learn/course/connecting-to-a-mongodb-database/lesson-1-using-mongodb-connection-strings/learn?client=customer&amp;wvideo=eh4048sbrf"></a></p>

   3. We will use the URI string to create Secret Block on Prefect.
   
![prefect secret](images/prefect_secret.png)

![prefect secret_1](images/prefect_secret_1.png)

Google Cloud and BiqQuery

It is assumed that you have: 
  - created an account in Google Cloud
  - Created a [project](https://cloud.google.com/resource-manager/docs/creating-managing-projects).
  - Access control for projects with [IAM](https://cloud.google.com/resource-manager/docs/access-control-proj).
  - Created a [service account](https://cloud.google.com/iam/docs/service-account-overview).
  - Created a [Prefect account](https://app.prefect.cloud/).
  
 
  1. Create a GCS Bucket and BQ Dataset using Terraform. Detailed instructions [here](terraform/README-TERRAFORM.md).
     
  2. Create a GCP Credentials block on Prefect.

  ![gcp creds](images/prefect_gcp_creds.png)
  ![gcp creds_1](images/prefect_gcp_creds_1.png)

  3. Create GCS Bucket block on Prefect.

  ![gcs bucket](images/prefect_gcs_bucket.png)
  ![gcs bucket_1](images/prefect_gcs_bucket_1.png)

  4. Create a Github block on Prefect.

  ![github](images/prefect_github.png)
  ![github_1](images/prefect_github_1.png)

  5.  Create a Prefect Deployment
      - On your terminal, run `prefect_deployment.py`
      - Check on Prefect Cloud UI to verify that a deployment has been created.

  6. Login to prefect cloud then start Prefect Agent
      - Run `prefect cloud login`
      - Follow the prompts.
      - Run `prefect agent start -q default`
       ![prefect agent](images/prefect%20_agent.png)
  7. Run Prefect Deployment
      - Run `prefect deployment run etl-mongodb-to-bq/airbnb-flow --params '{"db_name":"sample_airbnb", "coll_name": "listingsAndReviews"}'` 
    
        ![Alt text](images/prefect_completed.png)

## Metabase Dashboard

The listings and Reviews dashboard consists of 6 cards. I choose this
questions to answer to demonstrate proof-of-skill.

> ##### Note: The data does not include date and time of check-in and check-out. 
> ##### Maybe for privacy reasons.


  1. How many total room does listings and reviews have?
    > There are 5,555 listings on this dataset, just a sample subset.
  2. How many rooms have no Wifi and Internet amenities in listings_and_reviews?
    > For those who wants to disconnect, your best bet is to got to Brazil, 
      as more listings there have no Wifi and Internet. 
      Or maybe there are no services there 🤷🏽‍♂️
  3. Table of address_country and number of amenities and number of rooms
    > Hmmm. There are just 3 types of room in Airbnb.
  4. What is the total revenue generated per country?
    > Based on price column, Hong Kong 🇭🇰 got the larger share and China 🇨🇳 has the least.
  5. What room types are present in the sample_airbnb.listingsAndReviews collection?
    > Hmmm. There are just 3 types of room in Airbnb based on this dataset. I bet there more...
  6. Number of first reviews over time?
    > Using the first review column. It seems people pen 🔏 their first review on January's.


![dashboard_metabase](images/dashboard_metabase.png)
> ###### Due to time constraints, I was not able to do, much complex graphs and CTE's. I will
> ###### update this if time permits :)


## Outro

In conclusion, we have successfully created an efficient and automated data pipeline that fetches data from MongoDB, loads it into GCS and BQ, and visualizes it using Metabase. 

This pipeline has enabled us to gain valuable insights from our data and can make informed decisions based on these insights.

Finger-cross that this pipeline can pass the peer review. 🤞🏽




