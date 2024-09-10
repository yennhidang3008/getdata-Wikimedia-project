# Wikimedia ETL Pipeline

### INTRODUCTION
This project implements an ETL pipeline to extract pageviews data from Wikimedia dumps, transform the data, load it into a database, and finally store the processed data in AWS S3 for further analysis.

### SYSTEM ARCHITECTURE

### THE ETL PIPELINE IN THIS PROJECT CONSISTS OF THE FOLLOWING KEY COMPONENTS:
- `Data Source`: The URL [Wikimedia pageviews](https://dumps.wikimedia.org/other/pageviews/) provides access to a collection of data files containing pageviews statistics for various Wikimedia projects, including Wikipedia. The data is organized by year, starting from 2015 to 2024, and includes detailed records of the number of views for individual articles over time. This resource is valuable for researchers, developers, and analysts interested in understanding user engagement and trends on Wikimedia platforms.
- `Apache Airflow`: This tool is responsible for orchestrating the entire data pipeline, fetching data from the URL, and storing it in a `PostgreSQL` database.
- `Docker`: It containerizes both Airflow and PostgreSQL, ensuring a consistent and isolated environment. This simplifies deployment, manages dependencies, and allows for easy scaling, making development and maintenance more efficient.
- `AWS S3`: S3 provides scalable and durable storage for the processed data, ensuring secure access and efficient data management for future analysis.

### TECHNOLOGIES
- Docker
- Python
- Apache Airflow
- PostgreSQL
- AWS S3
  
