# Airflow pipeline for Tipico

A project for building, testing, scheduling, and running an ELT data
pipeline as a SQL workflow using:

- [dbt-core](https://github.com/dbt-labs/dbt-core) used for the modeling of data
- [Apache Airflow](https://airflow.apache.org/docs/) used as a runner, scheduler, and orchestrator.
- [RedShift](https://docs.aws.amazon.com/redshift/) as a data warehouse option.
 ![image](https://github.com/sumanththota/demo-dbt/assets/30614314/b7a16ac1-ff59-40f7-9bd1-c297b31fea56)


## Getting started
- Install docker 
- pip install -r requirements.txt
- Go to directory /tipico/airflow where docker-compose.yaml is, and contains the project configuration
- run docker compose up
- Navigate to http://localhost:8080/ to access the Airflow UI

## Connect to Redshift Database
- Use any sql client to connect to the database
- create a redshift connector on airflow if not already present

## Running the pipeline
- Search for 'tipico_database_dag' and run it to create the staging table
- Search for 'tipico_dag' to pull the data from api and load it into the staging table on redshift

## Running the models
- Nagivate out of airflow and into the dbt directory 
- run command dbt run --models path:models/tipico_models
- find the newly created tables on redshift database

Handy dbt commands:
- `dbt compile`
- `dbt test`
- `dbt run`
- `dbt docs generate`
- `dbt docs serve`

## Data Flow Diagram
![image](https://github.com/vrashi/tipico/blob/dev/Tipico%20Data%20Flow.jpg)

## Database design
![image](https://github.com/vrashi/tipico/blob/dev/Tipico%20Database%20Design.jpg)

