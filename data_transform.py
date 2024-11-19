import logging
import pandas as pd
from typing import List, Dict

def transform_data(raw_df: pd.DataFrame) -> List[Dict]:
    """
    Transform raw data into a format suitable for BigQuery.
    
    Args:
        raw_df (pd.DataFrame): Raw data extracted from the API.
    
    Returns:
        List[Dict]: Transformed data ready for BigQuery.
    """
    try:
        # Create a copy of the raw DataFrame to avoid modifying the original data
        transformed_df = raw_df.copy()
        logging.info("Created a copy of the raw DataFrame.")

        # List of columns that contain nested data (arrays or None)
        # Ensure the column name matches exactly, considering sanitization if applied earlier
        nested_columns = ['lfm.content.tags']

        # Apply processing to each nested column
        for col in nested_columns:
            if col in transformed_df.columns:
                # Apply the processing function to handle nested structures
                transformed_df[col] = transformed_df[col].apply(process_nested_field)
                logging.info(f"Processed nested column '{col}'.")
            else:
                logging.warning(f"Column '{col}' does not exist in the DataFrame.")

        # Replace dots with ampersands in column names
        transformed_df = sanitize_column_names(transformed_df)
        logging.info("Replaced dots with ampersands in column names.")

        # Handle NaN values across the DataFrame
        transformed_df = handle_nan_values(transformed_df)
        logging.info("Handled NaN values successfully.")

        # Convert the transformed DataFrame to a list of dictionaries
        list_of_dicts = transformed_df.to_dict(orient='records')
        logging.info(f"Transformed {len(list_of_dicts)} records successfully.")

        return list_of_dicts

    except Exception as e:
        # Log the error with stack trace for better debugging
        logging.exception(f"Data transformation failed: {e}")
        raise

def sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize column names by replacing dots with ampersands.
    
    Args:
        df (pd.DataFrame): DataFrame with original column names.
    
    Returns:
        pd.DataFrame: DataFrame with sanitized column names.
    """
    sanitized_columns = []
    for col in df.columns:
        # Replace dots with ampersands to avoid issues in BigQuery column naming
        sanitized = col.replace('.', '&')
        sanitized_columns.append(sanitized)
        logging.debug(f"Renamed column '{col}' to '{sanitized}'.")

    df.columns = sanitized_columns
    return df

def handle_nan_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle NaN values in the DataFrame by applying different strategies based on data type.
    
    Args:
        df (pd.DataFrame): DataFrame with potential NaN values.
    
    Returns:
        pd.DataFrame: DataFrame with NaN values handled.
    """
    # Example 1: Fill numeric columns with 0
    numeric_cols = df.select_dtypes(include=['number']).columns
    if not numeric_cols.empty:
        df[numeric_cols] = df[numeric_cols].fillna(0)
        logging.debug(f"Filled NaN values in numeric columns: {list(numeric_cols)} with 0.")

    # Example 2: Fill categorical/string columns with 'Unknown'
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns
    if not categorical_cols.empty:
        df[categorical_cols] = df[categorical_cols].fillna('Unknown')
        logging.debug(f"Filled NaN values in categorical columns: {list(categorical_cols)} with 'Unknown'.")

    # Example 3: Handle datetime columns by filling with a specific date
    datetime_cols = df.select_dtypes(include=['datetime']).columns
    if not datetime_cols.empty:
        specific_date = pd.Timestamp('2000-01-01')  # Replace with desired default date if necessary
        df[datetime_cols] = df[datetime_cols].fillna(specific_date)
        logging.debug(f"Filled NaN values in datetime columns: {list(datetime_cols)} with {specific_date}.")

    return df

def process_nested_field(input_list: list) -> dict:
    """
    Parses a list of strings formatted as "<key>: <value>" and returns a dictionary.
    Returns an empty dict if the input is not a list, the list is empty, or no elements match the format.
    
    Args:
        input_list (list): The input list containing string elements.
        
    Returns:
        dict or empty dict: A dict with keys and concatenated values or empty dict.
    """
    if not isinstance(input_list, list):
        logging.debug("Not-list value encountered. Returning None.")
        return {}
    
    if not input_list:
        # Return empty dict if the list is empty
        logging.debug("Empty list encountered. Returning None.")
        return {}

    processed_dict = {}

    for item in input_list:
        try:
            if isinstance(item, str):
                parts = item.split(":", 1)
                if len(parts) == 2:
                    key = parts[0].strip() or "untitled"
                    value = parts[1].strip()
                    if key in processed_dict:
                        processed_dict[key] += f"//{value}"
                    else:
                        processed_dict[key] = value
                else:
                    if "untitled" in processed_dict:
                        processed_dict['untitled'] += f"//{item}"
                    else:
                        processed_dict['untitled'] = item

        except Exception as e:
            logging.exception(f"Error processing item: {e}")
            raise

    if not processed_dict:
        # Return empty dict if no valid "key: value" pairs are found
        logging.debug("List with no <key>: <value> pair elements encountered. Returning None.")
        return {}

    return processed_dict