import json
import pandas as pd
from typing import List, Dict

def transform_data(raw_df: pd.DataFrame, transform_config) -> List[Dict]:
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
        print(json.dumps({
            "severity": "DEBUG",
            "message": "Created a copy of the raw DataFrame."
        }))

        # Remove rows containing "unauthorized" in any column
        transformed_df = transformed_df[~transformed_df.isin(["unauthorized"]).any(axis=1)]
        print(json.dumps({
            "severity": "DEBUG",
            "message": "Removed rows containing 'unauthorized'"
        }))
        
        # Remove authorized columns
        columns = list(transform_config["group_by"].keys()) + list(transform_config["meta_dimensions"].keys()) + list(transform_config["metrics"].keys())
        transformed_df = transformed_df[columns]
        # Cast each column to the correct data type
        columns_to_cast = {**transform_config["group_by"], **transform_config["meta_dimensions"], **transform_config["metrics"]}
        for column, dtype in columns_to_cast.items():
            if column in transformed_df.columns:
                if dtype == "int64":
                    transformed_df[column] = pd.to_numeric(transformed_df[column], errors="coerce").fillna(0).astype("int64")
                elif dtype == "float64":
                    transformed_df[column] = pd.to_numeric(transformed_df[column], errors="coerce").fillna(0.0).astype("float64")
                elif dtype == "datetime64[ns]":
                    transformed_df[column] = pd.to_datetime(transformed_df[column], errors="coerce")
                elif dtype == "string":
                    transformed_df[column] = transformed_df[column].where(transformed_df[column].notna(), None).astype("string")

        # List of columns that contain nested data (arrays or None)
        # Ensure the column name matches exactly, considering sanitization if applied earlier
        nested_columns = ['lfm.content.tags']
        # Apply processing to each nested column
        for col in nested_columns:
            if col in transformed_df.columns:
                # Apply the processing function to handle nested structures
                transformed_df[col] = transformed_df[col].apply(lambda x: process_nested_field(x, col))
                # Convert json column in columns
                aux_df = transformed_df[col].apply(pd.Series).where(lambda x: x.notna(), None)
                # Concat transformed_df and aux_df
                transformed_df = pd.concat([transformed_df, aux_df], axis=1)
                # Delete original column
                transformed_df = transformed_df.drop(col, axis=1)
                print(json.dumps({
                    "severity": "DEBUG",
                    "message": f"Processed nested column '{col}'."
                }))
            else:
                print(json.dumps({
                    "severity": "DEBUG",
                    "message": f"Column '{col}' does not exist in the DataFrame."
                }))

        # Handle datetime columns
        if 'lfm.fact.date_str' in transformed_df.columns:
            transformed_df['lfm.fact.date_str'] = transformed_df['lfm.fact.date_str'].dt.strftime("%Y-%m-%d")
        timestamp_columns = ['lfm.content.posted_on_datetime', 'lfm.fact.window_start_date', 'lfm.fact.window_end_date']
        for timestamp_column in timestamp_columns:
            if timestamp_column in transformed_df.columns:
                transformed_df[timestamp_column] = transformed_df[timestamp_column].dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Replace dots with ampersands in column names
        transformed_df = sanitize_column_names(transformed_df)
        print(json.dumps({
            "severity": "DEBUG",
            "message": "Replaced dots with ampersands in column names."
        }))

        # Convert the transformed DataFrame to a list of dictionaries
        list_of_dicts = transformed_df.to_dict(orient='records')
        print(json.dumps({
            "severity": "INFO",
            "message": f"Transformed {len(list_of_dicts)} records successfully."
        }))

        return list_of_dicts

    except Exception as e:
        # Log the error with stack trace for better debugging
        print(json.dumps({
            "severity": "ERROR",
            "message": f"Data transformation failed: {e}"
        }))
        return raw_df

def process_nested_field(input_list: list, field_name: str) -> dict:
    """
    Parses a list of strings formatted as "<key>: <value>" and returns a dictionary.
    Returns an empty dict if the input is not a list, the list is empty, or no elements match the format.
    
    Args:
        input_list (list): The input list containing string elements.
        
    Returns:
        dict or empty dict: A dict with keys and concatenated values or empty dict.
    """
    if not isinstance(input_list, list):
        return {}
    
    if not input_list:
        return {}

    processed_dict = {}

    for item in input_list:
        try:
            if isinstance(item, str):
                parts = item.split(":", 1)
                if len(parts) > 1:
                    key = field_name + '.' + parts[0].strip().replace(' ', '_')
                    value = parts[1].strip()
                    if key in processed_dict:
                        processed_dict[key] += f"//{value}"
                    else:
                        processed_dict[key] = value
                else:
                    key = field_name + ".untitled"
                    if key in processed_dict:
                        processed_dict[key] += f"//{item}"
                    else:
                        processed_dict[key] = item

        except Exception as e:
            print(json.dumps({
                "severity": "WARNING",
                "message": f"Error processing item: {e}"
            }))
            raise

    if not processed_dict:
        # Return empty dict if no valid "key: value" pairs are found
        return {}

    return processed_dict

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
        print(json.dumps({
            "severity": "DEBUG",
            "message": f"Renamed column '{col}' to '{sanitized}'."
        }))

    df.columns = sanitized_columns
    return df