import requests
import pandas as pd

from src.config import RAW_DIR
from src.logger import logger


BASE_URL = "https://environment.data.gov.uk/flood-monitoring/id/stations"
DEFAULT_LIMIT = 500
TIMEOUT_SECONDS = 30

#Keep only columns that exist in the response
KEEP_COLUMNS = [
    "stationReference",
    "label",
    "lat",
    "long",
    "riverName",
    "catchmentName",
    "town",
    "measures.latestReading.value",
    "measures.latestReading.dateTime",
]
#Lets rename the columns
RENAME_MAP = {
    "stationReference": "station_id",
    "label": "station_name",
    "lat": "latitude",
    "long": "longitude",
    "riverName": "river_name",
    "catchmentName": "catchment_name",
    "town": "town",
    "measures.latestReading.value": "latest_reading",
    "measures.latestReading.dateTime": "latest_reading_time",
}


def fetch_stations(limit: int = DEFAULT_LIMIT) -> pd.DataFrame:
    """
    Pulls Live monitoring stations data from the UK Environment Agency Flood Monitoring API.

    Args:
        limit: Maximum number of stations to fetch. Default is 500.

    Returns:
        DataFrame containing station metadata and any available latest reading fields.
    """
    logger.info("Fetching up to %s stations from EA API", limit)

    with requests.Session() as session:
        response = session.get(
            BASE_URL,
            params={"_limit": limit},
            timeout=TIMEOUT_SECONDS,
        )
        response.raise_for_status()

    items = response.json().get("items", [])

    if not items:
        raise ValueError("EA API returned no station records")

    logger.info("Successfully fetched %s stations from EA API", len(items))

    return pd.json_normalize(items)


def clean_stations_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardise EA station data.
    """
    if df.empty:
        raise ValueError("Input DataFrame is empty")

    keep_columns = [col for col in KEEP_COLUMNS if col in df.columns]

    df = df[keep_columns].copy()
    df = df.rename(columns=RENAME_MAP)

    for col in ["latitude", "longitude", "latest_reading"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "latest_reading_time" in df.columns:
        df["latest_reading_time"] = pd.to_datetime(
            df["latest_reading_time"],
            errors="coerce",
            utc=True,
        )

    if "station_id" in df.columns:
        df = df.drop_duplicates(subset="station_id")

    logger.info(
        "Cleaned station data: %s rows, %s columns",
        len(df),
        len(df.columns),
    )

    return df


def save_stations_data(
    df: pd.DataFrame,
    filename: str = "ea_stations.parquet",
) -> None:
    """
    Save cleaned station data to a Parquet file.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    output_path = RAW_DIR / filename

    df.to_parquet(output_path, index=False)

    logger.info("Saved %s stations to %s", len(df), output_path)


def main() -> None:
    raw_df = fetch_stations()
    cleaned_df = clean_stations_data(raw_df)
    save_stations_data(cleaned_df)

    print(cleaned_df.head(10))
    print(f"\nShape: {cleaned_df.shape}")
    print(f"\nColumns: {list(cleaned_df.columns)}")
    print(f"\nMissing values:\n{cleaned_df.isna().sum()}")


if __name__ == "__main__":
    main()