import logging
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
        logging.info("Created a copy of the raw DataFrame.")
        
        # Remove authorized columns
        # columns = list(transform_config["group_by"].keys()) + list(transform_config["meta_dimensions"].keys()) + list(transform_config["metrics"].keys())
        # transformed_df = transformed_df[columns]
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
                logging.info(f"Processed nested column '{col}'.")
            else:
                logging.warning(f"Column '{col}' does not exist in the DataFrame.")

        # Handle datetime columns
        if 'lfm.fact.date_str' in transformed_df.columns:
            transformed_df['lfm.fact.date_str'] = transformed_df['lfm.fact.date_str'].dt.strftime("%Y-%m-%d")
        timestamp_columns = ['lfm.content.posted_on_datetime', 'lfm.fact.window_start_date', 'lfm.fact.window_end_date']
        for timestamp_column in timestamp_columns:
            if timestamp_column in transformed_df.columns:
                transformed_df[timestamp_column] = transformed_df[timestamp_column].dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Replace dots with ampersands in column names
        transformed_df = sanitize_column_names(transformed_df)
        logging.info("Replaced dots with ampersands in column names.")

        # Convert the transformed DataFrame to a list of dictionaries
        list_of_dicts = transformed_df.to_dict(orient='records')
        logging.info(f"Transformed {len(list_of_dicts)} records successfully.")

        return list_of_dicts

    except Exception as e:
        # Log the error with stack trace for better debugging
        logging.exception(f"Data transformation failed: {e}")
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
            logging.exception(f"Error processing item: {e}")
            raise

    if not processed_dict:
        # Return empty dict if no valid "key: value" pairs are found
        logging.debug("List with no <key>: <value> pair elements encountered. Returning None.")
        return {}

    return processed_dict


# def handle_nan_values(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Handle NaN values in the DataFrame by applying different strategies based on data type.
    
#     Args:
#         df (pd.DataFrame): DataFrame with potential NaN values.
    
#     Returns:
#         pd.DataFrame: DataFrame with NaN values handled.
#     """

#     df['lfm.content.is_paid'] = df['lfm.content.is_paid'].astype(bool)
#     df['tiktok.post.likes'] = df['tiktok.post.likes'].astype(int)

#     # Example 1: Fill numeric columns with 0
#     numeric_cols = df.select_dtypes(include=['number']).columns
#     if not numeric_cols.empty:
#         df[numeric_cols] = df[numeric_cols].fillna(0.0)
#         logging.debug(f"Filled NaN values in numeric columns: {list(numeric_cols)} with 0.")

#     # Example 2: Fill categorical/string columns with 'Unknown'
#     categorical_cols = df.select_dtypes(include=['object', 'category']).columns
#     if not categorical_cols.empty:
#         df[categorical_cols] = df[categorical_cols].fillna('Unknown')
#         logging.debug(f"Filled NaN values in categorical columns: {list(categorical_cols)} with 'Unknown'.")

#     # Example 3: Handle datetime columns by filling with a specific date
#     datetime_cols = df.select_dtypes(include=['datetime']).columns
#     if not datetime_cols.empty:
#         specific_date = pd.Timestamp('2000-01-01')  # Replace with desired default date if necessary
#         df[datetime_cols] = df[datetime_cols].fillna(specific_date)
#         logging.debug(f"Filled NaN values in datetime columns: {list(datetime_cols)} with {specific_date}.")

#     return df

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