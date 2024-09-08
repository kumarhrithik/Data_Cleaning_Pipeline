import pandas as pd
import logging
from typing import Union

logger = logging.getLogger(__name__)

def load_dataset(file_path: str) -> Union[pd.DataFrame, str]:
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        logger.error(f"Error loading dataset: {e}")
        return str(e)
