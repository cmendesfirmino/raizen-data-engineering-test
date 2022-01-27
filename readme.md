# Raízen Data Engineering Test

This is my version of solution of [raizen-test](https://github.com/raizen-analytics/data-engineering-test). The goals was:

1. Download XLS file
2. Read pivot cache data from file.
3. Retrieve information of two tables:
 - Sales of oil derivative fuels by UF and product
 - Sales of diesel by UF and type
4. Make a pipeline to automate this operation

I used Apache Airflow, hosted in docker container, using Bash Operator to download data and convert to libreoffice format, to get pivot cached. Python script to data gathering, mainly with Pandas library. Save the output table in Parquet file format. After then I've uploaded the parquet file to AWS S3 bucket and persist in Postgres database.

## Installation

Pre requisites:

1. Docker
2. Docker Compose
3. AWS S3 Account
 
Clone this repository and go to directory.

```bash
git clone https://github.com/cmendesfirmino/raizen-data-engineering-test.git
cd raizen-data-engineering-test/
```

Create logs folder

```bash
mkdir ./logs 
```
Build the docker image using docker compose and run init airflow.
```bash
docker-compose build
docker-compose up airflow-init
```

After than we can up Airflow and others container services with docker compose.
```bash
docker-compose up -d
```
After then you can access Aiflow at localhost:8080. And will required user access and pasword, type airflow to both.

Before run this example dag, you have to add some Variables:

From AWS access:
1. AWS_ACCESS_KEY_ID
2. AWS_SECRET_ACCESS_KEY
3. S3_NAME_BUCKET
4. S3_HOST_BUCKET

To persist data to postgres, you have to add those Variables and create one database in Postgres, I used Dbeaver to do this, and I expose the port 5432 at localhost to access the Postgres.
1. POSTGRES_USER (I used the same as Airflow)
2. POSTGRES_PASSWORD
3. POSTGRES_SERVER ('postgres')
4. POSTGRES_DW

And now this dag could be run.

Thanks for this opportunity.


## License
[MIT](https://choosealicense.com/licenses/mit/)
