# PySpark Pipeline Project

This project demonstrates a data pipeline that processes sales data using PySpark and stores it in a PostgreSQL database. The pipeline reads a CSV file, processes the data using PySpark, and then writes the data to a PostgreSQL database.

## Project Structure

## Features

- Reads sales data from a CSV file.
- Processes the data using PySpark for schema inference and data transformation.
- Writes the processed data into a PostgreSQL database.
- GitHub Actions CI/CD pipeline for continuous integration.

## Prerequisites

- Python 3.11
- [PySpark](https://spark.apache.org/downloads.html)
- PostgreSQL
- Git

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/cleorhk/Pyspark-pipeline.git
   cd pyspark_pipeline_project
python -m venv venv
source venv/bin/activate   # For Linux/macOS
venv\Scripts\activate      # For Windows
pip install -r requirements.txt
