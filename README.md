# Airflow pipeline for Tipico

A project for building, testing, scheduling, and running an ELT data
pipeline as a SQL workflow using:

- [dbt-core](https://github.com/dbt-labs/dbt-core)
- [Apache Airflow](https://airflow.apache.org/docs/) used as runner, scheduler, and orchestrator.
- [RedShift](https://docs.aws.amazon.com/redshift/) as a data warehouse option.
 ![image](https://github.com/sumanththota/demo-dbt/assets/30614314/b7a16ac1-ff59-40f7-9bd1-c297b31fea56)


## Getting started
- pip install -r requirements.txt

Run `bash setup.sh` once to install project dependencies and configure the desired data warehouse and agent connection.

Run the following target commands to execute the desired SQL workflow operation:
- `dbt compile`
- `dbt test`
- `dbt run`
- `dbt docs generate`
- `dbt docs serve`

## Data Flow Diagram
![image](https://github.com/vrashi/tipico/blob/dev/Tipico%20Data%20Flow.jpg)

## Database design
![image](https://github.com/vrashi/tipico/blob/dev/Tipico%20Database%20Design.jpg)

