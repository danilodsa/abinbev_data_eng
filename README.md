# AbInbev challenge development

## Design Choices
- Medallion Architecture: Data is stored in three layers (Bronze, Silver, Gold) for better organization and scalability.
- Apache Airflow: Used for orchestration due to its robust scheduling, monitoring, and error-handling capabilities. Airflow allows for easy retries, task dependencies, and pipeline visualization. Provides a user-friendly UI for monitoring pipeline runs and logs.
- Modularity: The project is organized into separate scripts for fetching, transforming, and aggregating data, making it easy to maintain and extend.
- Containerization: Docker is used to containerize the application, ensuring consistency across environments and simplifying deployment.

- Pandas was used due to the small dataset size, being sufficient to process this amount of data. Also used due to ease of containerization and integration with airflow. PySpark was used in other versions, running well locally but presenting some incompatibilities when containerized together with airflow.

- To test data processing steps, pytest was used due to the familiarity.

## Project structure
ABINBEV_DATA_ENG/
├── dags/                  # Airflow DAGs
│   └── breweries_pipeline.py
├── data/                  # Data lake, medallion architecture (bronze, silver, gold layers will be stored here)
├── tasks/                 # ETL task scripts
│   ├── __init__.py        
│   ├── bronze_to_silver.py
│   ├── data_ingestion.py
│   ├── silver_to_gold.py
├── tests/                 # Test cases
├── .env                   # Environment variables
├── .gitignore             # Git ignore file
├── docker-compose.yml     # Docker Compose configuration
├── Dockerfile             # Docker image definition
├── main.py                # For local testing
├── README.md              # Documentation
└── requirements.txt       # Python dependencies

## How to run it
docker-compose up --build



## Support material used
https://airflow.apache.org/docs/apache-airflow/2.10.0/installation/prerequisites.html
https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml



