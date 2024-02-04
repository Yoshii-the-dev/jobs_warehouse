

## What this project is all about, what goals it tries to achieve and how users can benefit from it:
Traditional job boards excel in providing job listings, but they commonly lack statistical data that could empower users with valuable insights. As a result, job seekers may find it challenging to make informed decisions about their career paths, and businesses may struggle to align their hiring strategies with market trends.

Some benefits of a data-driven approach:
 - By analyzing data over time users can identify trends in job demand, industry preferences, and skill requirements, helping job seekers and employers stay informed
 - Users can understand the most in demand skills for specific roles, aiding them in tailoring their skillsets for better career prospects.
 - Analyzing job data based on geographical locations can assist job seekers in identifying regions with higher job opportunities in their desired field.
 - By aggregating data on employers and industries, the project can offer insights into companies actively hiring, industry growth, and market dynamics.

This project focuses on aggregating applications from various job boards and it's main scope is on an achitectural part, it builds a foundation that could scale well and provide a seamless analytics experience for it's users. Though a simple example of a possible analytical dashboard will be demonstrated as well.

- - - -
### What modeling technique was chosen / technologies used and why. Let's dive into the internals.
Basically, the whole project is divided into several microservices for better isolation and maintenance. A well-established technology widely acknowledged for this purpose is the utilization of Docker containers. We built three custom images based on Docker files: for data extraction from job boards (Scrapy+Selenium is a great fit for our goals), for the data warehouse itself (we use PostgresDB), and for data streaming and processing (PyFlink in our case).

Here is a little sketch of our architecture:
![Alt text](image-1.png)

### Why PostgresDB?
It's open source and has a generous free-tier serverless provider to deploy on, read-write optimized, and well-suited for OLAP systems. We could also use some NoSQL DB, such as MongoDB, early in the pipeline to avoid schema compatibility checks.
### Why PyFlink?
Though not as popular as Spark and not really beginner-friendly, I like it for the unified processing model, which seamlessly integrates batch and stream processing. In our app, we consume bounded streams of data from web scrapers, so we stick with batch processing, which is more optimized for this purpose. However, for monitoring/audit tasks that require prompt responsiveness, we can easily switch to stream processing. Flink also supports a wide range of different connectors.

### Regarding the choice of achitecture:
Our data product is divided into two zones: Landing and Presentational. <br> 
- The Landing zone is a part of the pipeline where our data turns from raw to clean and deduplicated and gets more feature-rich 
- The Presentational zone is represented by a so-called star schema (it consists of a denormalized 2NF central table or 'facts table,' surrounded by several tables in the 3NF that describe the facts; they are also called dimensions). To be more space-savvy, we decided to implement our fact table in the form of Type 1 SCD (Upsert). <br>
> Nevertheless, for our project, the OBT (One Big Table) architecture is also viable. But Kimball's star schema promotes better ETL code conceptualization and organization, making it easier for end users to navigate and consume less disk space, which was of utmost importance here.

-----
### How to run it locally:
> __Prerequisites__: Docker Desktop, git

After these installed on your PC, open your code editor and navigate to the directory, where you want to store our repository, then write the following command in the terminal:
```
git clone https://github.com/Yoshii-the-dev/jobs_warehouse.git
```


And:
```
docker-compose up -d
```
It will build underlying images and start the containers.
Oh, almost forgot:
Fill into your credentials in the .env file, they will be later used by Docker containers. You can fing an example in the repository
> Alternatively, you could just fork it into your personal github, create respective repository secrets and integrate it with github actions, if you want to run this app on a schedule.

----
### Demo of Analytical Dashboard built with Dash/Plotly
![Alt text](github_gif.gif)
