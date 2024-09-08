import pandas as pd
import numpy as np
import re
from typing import Callable, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DataPipelineTask:
    def __init__(self, name: str, func: Callable):
        self.name = name
        self.func = func

# Atomic Task Library
task_registry: Dict[str, DataPipelineTask] = {}

class DataPipeline:
    def __init__(self, tasks: Dict[str, DataPipelineTask]):
        self.tasks = tasks
        self.results: List[Dict[str, Union[str, Dict]]] = []

    def run(self, df: pd.DataFrame, task_list: List[Dict[str, str]]) -> List[Dict[str, Union[str, Dict]]]:
        for task_config in task_list:
            task_name = task_config.get('name')
            task = self.get_task(task_name)
            if task:
                try:
                    result = task.func(df, task_config)
                    result = self.convert_numpy_types(result)
                    self.results.append(result)
                except Exception as e:
                    logger.error(f"Error running task {task_name}: {e}")
                    self.results.append({"task": task_name, "error": str(e)})
        return self.results

    def get_task(self, name: str) -> Optional[DataPipelineTask]:
        return self.tasks.get(name, None)

    def convert_numpy_types(self, result: Union[Dict, List, np.generic, np.int64, np.float64, str]) -> Union[Dict, List, float, int, str]:
        if isinstance(result, dict):
            return {key: self.convert_numpy_types(value) for key, value in result.items()}
        elif isinstance(result, list):
            return [self.convert_numpy_types(item) for item in result]
        elif isinstance(result, (np.generic, np.int64, np.float64)):
            return result.item()
        else:
            return result

# Utility Functions for Data Cleaning Tasks

def check_missing_values(df: pd.DataFrame, config: Dict[str, Optional[List[str]]]) -> Dict[str, Union[str, Dict[str, Dict[str, Union[int, str, List[Dict[str, Union[int, str]]]]]]]]:
    columns = config.get("columns", df.columns.tolist())
    result = {}
    
    for col in columns:
        if col not in df.columns:
            result[col] = {"error": f"Column '{col}' not found"}
            continue
        
        # Identify missing values
        missing_indices = df[df[col].isna()].index.tolist()
        missing_count = len(missing_indices)
        
        # Collect missing value indices and placeholder
        missing_entries = [{'index': idx, 'value': 'NaN'} for idx in missing_indices]
        
        result[col] = {
            "missing_count": missing_count,
            "status": "OK" if missing_count == 0 else f"{missing_count} missing values",
            "missing_values": missing_entries if missing_count > 0 else []
        }
    return {"task": "check_missing_values", "result": result}


def check_duplicates(df: pd.DataFrame, config: Dict[str, Optional[List[str]]]) -> Dict[str, Union[str, Dict[str, Union[int, str]]]]:
    columns = config.get("columns", df.columns.tolist())
    id_column = config.get("id", None)  # Retrieve the id column from config

    if not columns:
        return {"error": "No columns specified for duplicate check", "task": "check_duplicates"}

    # Exclude rows with empty values in the specified columns
    if id_column:
        non_empty_df = df.dropna(subset=[id_column] + columns)
    else:
        non_empty_df = df.dropna(subset=columns)

    # Identify duplicate rows in the filtered DataFrame
    duplicate_rows = non_empty_df[non_empty_df.duplicated(subset=columns, keep=False)]
    
    if duplicate_rows.empty:
        return {"result": {"duplicate_count": 0, "duplicate_rows": [], "status": "OK"}, "task": "check_duplicates"}
    
    if id_column and id_column in non_empty_df.columns:
        # Include id in the result
        duplicate_rows = duplicate_rows[[id_column] + columns]
    else:
        id_column = None  # Reset id_column if it's not in the dataframe

    # Collect indices and values of duplicate rows
    grouped_duplicates = duplicate_rows.groupby(level=0).apply(lambda x: x[id_column].tolist() if id_column else x[columns].iloc[0].to_dict()).to_dict()

    # Prepare the result in the desired format
    duplicate_list = [{"index": idx, "value": grouped_duplicates[idx][0] if id_column else grouped_duplicates[idx]} for idx in grouped_duplicates]
    for item in duplicate_list:
        if isinstance(item["value"], dict) and "ID" in item["value"]:
            item["value"] = item["value"]["ID"]
    duplicate_count = len(duplicate_rows)

    result = {
        "duplicate_count": duplicate_count,
        "duplicate_rows": duplicate_list,
        "status": "OK" if duplicate_count == 0 else f"{duplicate_count} duplicates found"
    }
    
    return {"task": "check_duplicates", "result": result}


def check_date_formats(df: pd.DataFrame, config: Dict[str, Optional[List[str]]]) -> Dict[str, Union[str, Dict[str, Dict[str, str]]]]:
    date_columns = config.get("columns", ['Join_Date', 'Last_Login'])
    result = {}
    for col in date_columns:
        if col not in df.columns:
            result[col] = {"error": f"Column '{col}' not found"}
            continue
        
        inconsistent_data = []
        for index, value in df[col].items():
            try:
                pd.to_datetime(value, format='%d/%m/%Y', errors='raise')
            except Exception:
                inconsistent_data.append({'index': index, 'value': value})
        
        result[col] = {
            "date_format": "Inconsistent" if inconsistent_data else "Consistent",
            "inconsistent_entries": inconsistent_data if inconsistent_data else [],
            "status": "Inconsistent date format found" if inconsistent_data else "OK"
        }
    
    return {"task": "check_date_formats", "result": result}

def check_email_format(df: pd.DataFrame, config: Dict[str, str]) -> Dict[str, Union[str, Dict[str, Union[int, str, List[Dict[str, Union[int, str]]]]]]]:
    email_column = config.get("column", "Email")
    result = {}
    
    if email_column not in df.columns:
        result[email_column] = {"error": f"Column '{email_column}' not found"}
    else:
        # Check for invalid email format
        invalid_emails = df[email_column].apply(lambda x: not re.match(r"[^@]+@[^@]+\.[^@]+", str(x)))
        invalid_count = invalid_emails.sum()
        
        # Collect invalid email indices and values
        invalid_entries = [{'index': idx, 'value': df.at[idx, email_column]} for idx in invalid_emails[invalid_emails].index]
        
        result[email_column] = {
            "invalid_email_count": invalid_count,
            "status": "OK" if invalid_count == 0 else f"{invalid_count} invalid emails found",
            "invalid_emails": invalid_entries if invalid_count > 0 else []
        }
    return {"task": "check_email_format", "result": result}


def check_gender_consistency(df: pd.DataFrame, config: Dict[str, str]) -> Dict[str, Union[str, Dict[str, Union[int, str, List[Dict[str, Union[int, str]]]]]]]:
    gender_column = config.get("column", "Gender")
    result = {}
    
    if gender_column not in df.columns:
        result[gender_column] = {"error": f"Column '{gender_column}' not found"}
    else:
        # Standardize the gender column
        standardized_gender = df[gender_column].str.lower().replace({'male': 'M', 'female': 'F', '': 'Unknown'})
        
        # Identify inconsistent gender entries
        inconsistent_genders = standardized_gender != df[gender_column].str.lower()
        inconsistent_count = inconsistent_genders.sum()
        
        inconsistent_entries = [{'index': idx, 'value': df.at[idx, gender_column]} for idx in inconsistent_genders[inconsistent_genders].index]
        
        result[gender_column] = {
            "inconsistent_gender_count": inconsistent_count,
            "status": "OK" if inconsistent_count == 0 else f"{inconsistent_count} inconsistent genders found",
            "inconsistent_entries": inconsistent_entries if inconsistent_count > 0 else []
        }
    return {"task": "check_gender_consistency", "result": result}


def check_name_consistency(df: pd.DataFrame, config: Dict[str, str]) -> Dict[str, Union[str, Dict[str, Union[int, str, List[Dict[str, Union[int, str]]]]]]]:
    name_column = config.get("column", "Name")
    result = {}
    
    if name_column not in df.columns:
        result[name_column] = {"error": f"Column '{name_column}' not found"}
    else:
        # Identify blank names
        blank_names = df[name_column].isna() | (df[name_column] == '')
        blank_count = blank_names.sum()
        
        blank_entries = [{'index': idx, 'value': df.at[idx, name_column]} for idx in blank_names[blank_names].index]
        
        result[name_column] = {
            "blank_name_count": blank_count,
            "status": "OK" if blank_count == 0 else f"{blank_count} blank names found",
            "blank_entries": blank_entries if blank_count > 0 else []
        }
    return {"task": "check_name_consistency", "result": result}


def check_age_validity(df: pd.DataFrame, config: Dict[str, str]) -> Dict[str, Union[str, Dict[str, Union[int, str, List[Dict[str, Union[int, str]]]]]]]:
    age_column = config.get("column", "Age")
    result = {}
    
    if age_column not in df.columns:
        result[age_column] = {"error": f"Column '{age_column}' not found"}
    else:
        # Function to validate age
        def is_valid_age(x):
            if pd.isna(x):
                return False
            try:
                age = int(x)
                return 0 <= age <= 120
            except ValueError:
                return False
        
        # Identify invalid ages
        invalid_mask = df[age_column].apply(lambda x: not is_valid_age(x))
        invalid_entries = [{'index': idx, 'value': df.at[idx, age_column]} for idx in invalid_mask[invalid_mask].index]
        
        result[age_column] = {
            "invalid_age_count": len(invalid_entries),
            "status": "OK" if len(invalid_entries) == 0 else f"{len(invalid_entries)} invalid ages found",
            "invalid_entries": invalid_entries if len(invalid_entries) > 0 else []
        }
    return {"task": "check_age_validity", "result": result}

def add_task_to_library(task_name: str, task_func: Callable) -> str:
    if task_name in task_registry:
        return f"Task '{task_name}' already exists in the library."
    task_registry[task_name] = DataPipelineTask(task_name, task_func)
    return f"Task '{task_name}' has been added successfully."

def update_task_in_library(task_name: str, task_func: Callable) -> str:
    if task_name not in task_registry:
        return f"Task '{task_name}' does not exist. Please add it first."
    
    task_registry[task_name] = DataPipelineTask(task_name, task_func)
    return f"Task '{task_name}' has been updated successfully."
