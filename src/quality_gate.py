from pathlib import Path

import pandas as pd

from src.config import RAW_DIR
from src.logger import logger


REQUIRED_SCADA_COLUMNS = {
    "site_id",
    "timestamp",
    "level_m",
    "flow_lps",
    "pump_status",
}

REQUIRED_EA_COLUMNS = {
    "station_id",
    "station_name",
    "latitude",
    "longitude",
}

VALID_PUMP_STATUS = {"ON", "OFF", "AUTO"}


def check_required_columns(
    df: pd.DataFrame,
    required_columns: set[str],
    dataset_name: str,
) -> None:
    """
    Check that all required columns exist in the DataFrame.
    """
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required columns: {sorted(missing_columns)}"
        )


def apply_quality_rules(
    df: pd.DataFrame,
    rules: dict[str, pd.Series],
    dataset_name: str,
) -> pd.DataFrame:
    """
    Apply row level quality rules and remove rows that fail any rule.
    """
    for rule_name, mask in rules.items():
        failed_count = int(mask.sum())

        if failed_count > 0:
            logger.warning(
                "%s quality rule failed [%s]: %s rows",
                dataset_name,
                rule_name,
                failed_count,
            )

    failed_mask = pd.concat(rules, axis=1).any(axis=1)
    passed_df = df.loc[~failed_mask].copy()

    removed_count = len(df) - len(passed_df)

    logger.info(
        "%s quality gate complete: %s rows passed, %s rows removed",
        dataset_name,
        len(passed_df),
        removed_count,
    )

    return passed_df


def run_scada_quality_gate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate synthetic SCADA telemetry data.

    Checks:
        - Required columns exist
        - Site ID is present
        - Timestamp is valid
        - Level is not negative
        - Flow is not negative
        - Pump status is valid
        - Duplicate site and timestamp readings are removed

    Args:
        df: Raw SCADA DataFrame.

    Returns:
        DataFrame containing rows that passed all quality checks.
    """
    check_required_columns(df, REQUIRED_SCADA_COLUMNS, "SCADA")

    df = df.copy()

    logger.info("Running SCADA quality gate on %s rows", len(df))

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["level_m"] = pd.to_numeric(df["level_m"], errors="coerce")
    df["flow_lps"] = pd.to_numeric(df["flow_lps"], errors="coerce")

    rules = {
        "missing_site_id": df["site_id"].isna(),
        "invalid_timestamp": df["timestamp"].isna(),
        "invalid_level": df["level_m"].isna() | (df["level_m"] < 0),
        "invalid_flow": df["flow_lps"].isna() | (df["flow_lps"] < 0),
        "invalid_pump_status": ~df["pump_status"].isin(VALID_PUMP_STATUS),
        "duplicate_reading": df.duplicated(
            subset=["site_id", "timestamp"],
            keep="first",
        ),
    }

    return apply_quality_rules(df, rules, "SCADA")


def run_ea_quality_gate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate Environment Agency station data.

    Checks:
        - Required columns exist
        - Station ID is present
        - Station name is present
        - Latitude is valid
        - Longitude is valid
        - Duplicate station records are removed

    Args:
        df: Cleaned EA station DataFrame.

    Returns:
        DataFrame containing rows that passed all quality checks.
    """
    check_required_columns(df, REQUIRED_EA_COLUMNS, "EA stations")

    df = df.copy()

    logger.info("Running EA station quality gate on %s rows", len(df))

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    rules = {
        "missing_station_id": df["station_id"].isna(),
        "missing_station_name": df["station_name"].isna(),
        "invalid_latitude": df["latitude"].isna()
        | (df["latitude"] < -90)
        | (df["latitude"] > 90),
        "invalid_longitude": df["longitude"].isna()
        | (df["longitude"] < -180)
        | (df["longitude"] > 180),
        "duplicate_station": df.duplicated(
            subset=["station_id"],
            keep="first",
        ),
    }

    return apply_quality_rules(df, rules, "EA stations")


def load_scada_data(path: Path) -> pd.DataFrame:
    """
    Load synthetic SCADA data from CSV.
    """
    if not path.exists():
        raise FileNotFoundError(f"SCADA input file not found: {path}")

    logger.info("Loading SCADA data from %s", path)

    return pd.read_csv(path)


def load_ea_station_data(path: Path) -> pd.DataFrame:
    """
    Load EA station data from Parquet.
    """
    if not path.exists():
        raise FileNotFoundError(f"EA station input file not found: {path}")

    logger.info("Loading EA station data from %s", path)

    return pd.read_parquet(path)


def print_quality_summary(
    dataset_name: str,
    input_df: pd.DataFrame,
    output_df: pd.DataFrame,
) -> None:
    """
    Print a simple quality gate summary.
    """
    removed_count = len(input_df) - len(output_df)

    print(f"\n{dataset_name} Quality Gate Results")
    print(f"Input rows:  {len(input_df)}")
    print(f"Output rows: {len(output_df)}")
    print(f"Removed:     {removed_count}")
    print("\nSample output:")
    print(output_df.head(5))


def main() -> None:
    """
    Run quality gates for SCADA telemetry and EA station data.
    """
    scada_path = RAW_DIR / "synthetic_scada.csv"
    ea_path = RAW_DIR / "ea_stations.parquet"

    scada_df = load_scada_data(scada_path)
    clean_scada_df = run_scada_quality_gate(scada_df)

    ea_df = load_ea_station_data(ea_path)
    clean_ea_df = run_ea_quality_gate(ea_df)

    print_quality_summary("SCADA", scada_df, clean_scada_df)
    print_quality_summary("EA Stations", ea_df, clean_ea_df)


if __name__ == "__main__":
    main()