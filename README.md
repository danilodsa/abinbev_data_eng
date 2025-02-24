# abinbev_data_eng
AbInbev challenge development


Design Choices
Medallion Architecture: Data is stored in three layers (Bronze, Silver, Gold) for better organization and scalability.

Apache Airflow: Used for orchestration due to its robust scheduling, monitoring, and error-handling capabilities. Airflow allows for easy retries, task dependencies, and pipeline visualization. Provides a user-friendly UI for monitoring pipeline runs, task statuses, and logs.

PySpark: Used for data transformation and aggregation due to its scalability and ability to handle large datasets efficiently.

Modularity: The project is organized into separate scripts for fetching, transforming, and aggregating data, making it easy to maintain and extend.

Containerization: Docker is used to containerize the application, ensuring consistency across environments and simplifying deployment.



ABINBEV_DATA_ENG/
├── dags/                  # Airflow DAGs
│   └── breweries_pipeline.py
├── data/                  # Data lake (bronze, silver, gold layers will be stored here)
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


https://airflow.apache.org/docs/apache-airflow/2.10.0/installation/prerequisites.html

https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml

mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env


RODAR
docker-compose up --build