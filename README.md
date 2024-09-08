# Data Pipeline Project

## Overview
This project includes a Flask application for running data cleaning tasks on datasets. It supports adding and updating tasks dynamically.

## Setup
1. Install the required packages:
    ```
    pip install -r requirements.txt
    ```

2. Place your dataset in the project directory with the name `dataset.csv`.

3. Run the application:
    ```
    python run.py
    ```

## API Endpoints
- **POST /run-pipeline**: Run data cleaning tasks on the dataset.
- **POST /add-task**: Add a new data cleaning task.
- **POST /update-task**: Update an existing data cleaning task.