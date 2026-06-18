import requests
import pandas as pd
import logger
from src.config import RAW_DIR
from src.logger

BASE_URL = 'https://environment.data.gov.uk/flood-monitoring/id/stations'

def fetch_stations(limit: int= 500) -> pd.DataFrame:
    """
    Pulls Live monitoring stations data from the UK Environment Agency Flood MonitoringAPI.

    Args:
        limit: The maximum number of stations to fetch. Default is 500.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched stations data including location, river name and latest readings.

    Raises:
        requests.RequestException: If the API request fails.
    """
    logger.info('Fetching flood monitoring stations data from the UK Environment Agency API...', limit)
    
    try:
        response = requests.get(
            BASE_URL,
            params={'_limit': limit},
            timeout = 30
        )
        response.raise_for_status()  # Raise an exception for HTTP errors

        items = response.json().get('items', [])
        logger.info('Sucessfully  fetched stations data from the UK Environment Agency API.', len(items))

        return pd.json_normalize(items)  # Convert JSON to DataFrame
   
    except requests.RequestException as exc:
        logger.exception('EA API request failed: %s', exc)
        raise

def clean_stations_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the fetched stations data by removing unnecessary columns and renaming columns for clarity.

    Args:
        df (pd.DataFrame): The Raw DataFrame from fetch_stations
    
    Returns:
        Cleaned DataFrame with standardized column names

    """
    #Keep only columns that exist in the response
    keep = [col for col in [
        'stationReference',
        'label',
        'lat',
        'long',
        'riverName',
        'catchmentName',
        'town',
        'measures.latestReading.value',
        'measures.latestReading.dateTime',
      ] if col in df.columns]
    
    df = df[keep].copy()  # Create a copy of the DataFrame with only the necessary columns ]

    #Rename to cleaner column names
    rename_map = {
        'stationReference':                  'station_id',
        'label':                             'station_name',
        'lat':                               'latitude',
        'long':                              'longitude',
        'riverName':                         'river_name',
        'catchmentName':                     'catchment_name',
        'town':                              'town',
        'measures.latestReading.value':      'latest_reading',
        'measures.latestReading.dateTime':   'latest_reading_time',
    }
    df = df.rename(columns = {k: v for k, v in rename_map.items() if k in df.columns})

    logger.info(' Cleaned station data: %s rows, %s columns', len(df), len(df.columns))

    return df

def save_stations_data(df: pd.DataFrame, filename: str = 'ea_stations.parquet  ') -> None:
    """
    Save the cleaned station data to a Parquet file. Parquet preserves data types and is faster to read and write than CSV for large datasets.

    Args:
        df : Cleaned DataFrame to save
        filename : Output filename

    """
    output_path = RAW_DIR / filename
    df.to_parquet(output_path, index=False)
    logger.info('Saved cleaned station data to %s', len(df), output_path)

if __name__ == '__main__':
    # Step 1: Fetch raw data from the API
    raw_df = fetch_stations(limit=500)

    # Step 2: Clean and standardize the columns
    cleaned_df = clean_stations_data(raw_df)

    # Step 3: Save the cleaned data to a Parquet file
   save_stations(clean_df)

    #Step 4: Print a summary to confirm it worked
    print(clean_df.head(10))
    print(f'\nShape/: {clean_df.shape}')
    print(f'\nColumns: {list(clean_df.columns)}')
    print(f'\nMissing Values:\n{clean_df.isnull().sum()}')
