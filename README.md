# Data Cleaning Pipeline Project

## Overview
This project includes a Flask application for running data cleaning pipeline tasks on given dataset. It supports adding and updating tasks dynamically.

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

## Image References for Script Running
### I have only run the task for reference, specifically to check_date_format when comparing all entries
- **1. Running Pipeline for Reference**
![Running_Pipeline](https://github.com/user-attachments/assets/11eb067e-d4ea-4919-b724-e7f73edb8b90)

- **2. Adding a Task**
- ***only change the function name of check_date_format to checking_date_format***
![Add_Task_1](https://github.com/user-attachments/assets/d1b0779b-320b-45be-8043-31f0e87e50ef)

- **3. Running Pipeline Again to Verify Task Addition**
![After_Add_Task_1](https://github.com/user-attachments/assets/cae061bf-781e-4cf2-89a0-3b56cba8424d)

- **4. Updating a Task**
- ***only removed the index from checking_date_format***
![Update_Task_1](https://github.com/user-attachments/assets/a4adcc36-dc00-42a2-bf53-367daac1cb67)

- **5. Running Pipeline Again to Verify Task Update**
![After_Update_Task_1](https://github.com/user-attachments/assets/63925deb-63b2-4b9f-bf43-c96427c94b37)

