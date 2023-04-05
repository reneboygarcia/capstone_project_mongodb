# Capstone Project

## Preamble/Thank you.

I would like to express our appreciation for the smart, talented people of Data Engineering Zoomcamp and its helpful community for putting the course that helped us gain the skills and knowledge necessary to build this pipeline. Their guidance and expertise have been invaluable in our journey towards building efficient and effective data pipelines.


## Introduction

Data pipelines are critical to modern data infrastructure, as they allow businesses to process, analyze, and store vast amounts of data efficiently. A common use case involves moving data from MongoDB to Google Cloud Storage (GCS) to BigQuery (BQ) for further analysis. At Dashlabs.ai, we use Argo as our primary orchestration tool. However, for this specific use case, I will use Prefect as a test bed to build and monitor the pipeline. 


### Problem statement

As a final project to conclude the Data Engineering Zoomcamp. We will try to create a data pipeline extracting data stored in MongoDB, I will use the `airbnb sample dataset`.

The pipeline involves moving data from MongoDB to GCS to BQ for further analysis, followed by visualizing and analyzing the data with Metabase. The Google Cloud Storage and BigQuery Python APIs allow us to load and transform the data, respectively, while Metabase connects to the BQ warehouse and enables us to explore and analyze their data. By leveraging these tools and technologies, we are able to optimize our data pipeline and gain valuable insights from our data.

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
   
2. Create a GCP Credentials block on Prefect 

![gcp creds](images/prefect_gcp_creds.png)
![gcp creds_1](images/prefect_gcp_creds_1.png)

3. Create GCS Bucket block on Prefect
   
![gcs bucket](images/prefect_gcs_bucket.png)
![gcs bucket_1](images/prefect_gcs_bucket_1.png)

4. Create a Prefect Deployment
    - On your terminal, run `prefect_deployment.py`
    - Check on Prefect Cloud UI to verify that a deployment has been created.
5. 
## Technologies 

You don't have to limit yourself to technologies covered in the course. You can use alternatives as well:

* Cloud: AWS, GCP, Azure or others
* Infrastructure as code (IaC): Terraform, Pulumi, Cloud Formation, ...
* Workflow orchestration: Airflow, Prefect, Luigi, ...
* Data Wareshouse: BigQuery, Snowflake, Redshift, ...
* Batch processing: Spark, Flink, AWS Batch, ...
* Stream processing: Kafka, Pulsar, Kinesis, ...

If you use something that wasn't covered in the course, 
be sure to explain what the tool does.

If you're not certain about some tools, ask in Slack.


## Metabase Dashboard

You can build a dashboard with any of the tools shown in the course (Data Studio or Metabase) or any other BI tool of your choice. If you do use another tool, please specify and make sure that the dashboard is somehow accessible to your peers. 

Your dashboard should contain at least two tiles, we suggest you include:

- 1 graph that shows the distribution of some categorical data 
- 1 graph that shows the distribution of the data across a temporal line

Make sure that your graph is clear to understand by adding references and titles. 

![dashboard_metabase](images/dashboard_metabase.png)


## Peer review criteria

* Problem description
    * 0 points: Problem is not described
    * 1 point: Problem is described but shortly or not clearly 
    * 2 points: Problem is well described and it's clear what the problem the project solves
* Cloud
    * 0 points: Cloud is not used, things run only locally
    * 2 points: The project is developed in the cloud
    * 4 points: The project is developed in the cloud and IaC tools are used
* Data ingestion (choose either batch or stream)
    * Batch / Workflow orchestration
        * 0 points: No workflow orchestration
        * 2 points: Partial workflow orchestration: some steps are orchestrated, some run manually
        * 4 points: End-to-end pipeline: multiple steps in the DAG, uploading data to data lake
    * Stream
        * 0 points: No streaming system (like Kafka, Pulsar, etc)
        * 2 points: A simple pipeline with one consumer and one producer
        * 4 points: Using consumer/producers and streaming technologies (like Kafka streaming, Spark streaming, Flink, etc)
* Data warehouse
    * 0 points: No DWH is used
    * 2 points: Tables are created in DWH, but not optimized
    * 4 points: Tables are partitioned and clustered in a way that makes sense for the upstream queries (with explanation)
* Transformations (dbt, spark, etc)
    * 0 points: No tranformations
    * 2 points: Simple SQL transformation (no dbt or similar tools)
    * 4 points: Tranformations are defined with dbt, Spark or similar technologies
* Dashboard
    * 0 points: No dashboard
    * 2 points: A dashboard with 1 tile
    * 4 points: A dashboard with 2 tiles
* Reproducibility
    * 0 points: No instructions how to run code at all
    * 2 points: Some instructions are there, but they are not complete
    * 4 points: Instructions are clear, it's easy to run the code, and the code works

