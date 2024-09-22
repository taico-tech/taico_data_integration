import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_staging_data(dataframe):
    # Check for Duplicate Rows
    duplicate_rows = dataframe.duplicated()
    if duplicate_rows.any():
        logging.warning(f"Duplicate rows found in transformed data at indices: {duplicate_rows[duplicate_rows].index.tolist()}")

    # Row Count Validation
    expected_min_rows = 100
    if dataframe.shape[0] < expected_min_rows:
        logging.warning(f"Row count below expected minimum: {dataframe.shape[0]} < {expected_min_rows}")

    # Validate Unique Constraints with Detailed Index Reporting
    duplicates = dataframe['id'].duplicated()
    if duplicates.any():
        logging.error(f"Duplicate IDs found in rows: {duplicates[duplicates].index.tolist()}")
        return False

    # Validate Non-null Constraints and Log Offending Rows
    required_columns = ['id', 'date', 'channel', 'campaign', 'clicks', 'impressions', 'media_cost_eur']
    for col in required_columns:
        nulls = dataframe[col].isnull()
        if nulls.any():
            logging.error(f"Null values found in column '{col}' at rows: {nulls[nulls].index.tolist()}")
            return False

    # Validate Data Types with More Robust Type Checking
    try:
        assert pd.api.types.is_integer_dtype(dataframe['clicks']), "Clicks are not integer type."
        assert pd.api.types.is_float_dtype(dataframe['media_cost_eur']), "Media cost is not float type."
        assert pd.api.types.is_datetime64_any_dtype(dataframe['date']), "Date is not datetime type."
    except AssertionError as e:
        logging.error(f"Data type validation failed: {e}")
        return False

    # Advanced Range Checks with Contextual Error Messages
    if (dataframe['clicks'] < 0).any():
        logging.error("Negative values found in 'clicks'.")
        return False

    if (dataframe['media_cost_eur'] < 0).any():
        logging.error("Negative values found in 'media_cost_eur'.")
        return False

    # Consistency Checks with Detailed Context
    if (dataframe['media_cost_eur'] < dataframe['cpc']).any():
        logging.error("Media cost is less than cost per click for some records.")
        return False

    # Business Logic Checks with More Complex Conditions
    if (dataframe['revenue'] < dataframe['media_cost_eur']).any():
        problematic_rows = dataframe.loc[dataframe['revenue'] < dataframe['media_cost_eur']]
        logging.error(f"Revenue is less than media cost in rows: {problematic_rows.index.tolist()}")
        return False

    logging.info("All validations passed for transformed data.")
    return True

# Example usage
if __name__ == "__main__":
    # Assume `transformed_df` is the DataFrame after dbt transformations
    transformed_df = pd.DataFrame({
        'id': [1, 2, 3],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
        'channel': ['facebook', 'facebook', 'facebook'],
        'campaign': ['Campaign A', 'Campaign B', 'Campaign C'],
        'clicks': [100, 150, 200],
        'impressions': [1000, 1500, 2000],
        'media_cost_eur': [50.0, 75.0, 100.0],
        'cpc': [0.5, 0.5, 0.5],
        'revenue': [60.0, 80.0, 120.0]
    })

    if not validate_staging_data(transformed_df):
        logging.error("Validation failed for transformed data.")
    else:
        logging.info("Validation succeeded for transformed data.")
