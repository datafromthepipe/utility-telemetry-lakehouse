import numpy as np
import pandas as pd
from src.config import RAW_DIR
from src.logger import logger


def generate_site(site_id: str, start: str = '2026-01-01', periods: int = 96) -> pd.DataFrame:
    """
    Generate one day of synthetic SCADA sensor readings for a single site.
    Inject a rapid level rise anomaly between periods 10 and 15 to simulate
    a pump failure or pipe burst event.

    Args:
        site_id: Unique identifier for the monitoring site e.g. PS001
        start: Start date for the readings in YYYY-MM-DD format
        periods: Number of 15-minute intervals to generate. 96 = one full day

    Returns:
        DataFrame with columns: site_id, timestamp, level_m, flow_lps,
        pressure_bar, pump_status, source_system
    """
    rng = pd.date_range(start, periods=periods, freq='15min')

    # Generate base level readings around normal operating level
    base = np.random.normal(1.4, 0.15, periods)

    # Inject anomaly: rapid level rise over 75 minutes simulating a pump fault
    base[10:15] += np.linspace(0.2, 1.2, 5)

    df = pd.DataFrame({
        'site_id':       site_id,
        'timestamp':     rng,
        'level_m':       base.round(2),
        'flow_lps':      np.random.normal(32, 6, periods).round(2),
        'pressure_bar':  np.random.normal(2.6, 0.3, periods).round(2),
        'pump_status':   np.where(base > 2.0, 'ON', 'AUTO'),
        'source_system': 'synthetic_scada'
    })

    return df


def generate_all_sites(sites: list, start: str = '2026-01-01') -> pd.DataFrame:
    """
    Generate synthetic SCADA data for multiple monitoring sites
    and combine into a single DataFrame.

    Args:
        sites: List of site IDs to generate data for
        start: Start date for all sites

    Returns:
        Combined DataFrame with data for all sites
    """
    logger.info('Starting synthetic SCADA generation for %s sites', len(sites))

    frames = [generate_site(site_id=s, start=start) for s in sites]
    df = pd.concat(frames, ignore_index=True)

    logger.info('Generated %s rows across %s sites', len(df), df.site_id.nunique())

    return df


def save_scada_data(df: pd.DataFrame, filename: str = 'synthetic_scada.csv') -> None:
    """
    Save the generated SCADA data to the raw data folder.

    Args:
        df: DataFrame containing the generated SCADA readings
        filename: Output filename
    """
    output_path = RAW_DIR / filename
    df.to_csv(output_path, index=False)
    logger.info('Saved %s rows to %s', len(df), output_path)


if __name__ == '__main__':
    sites = ['PS001', 'PS002', 'PS003', 'SR001', 'SR002']
    df = generate_all_sites(sites=sites)
    save_scada_data(df)
    print(df.head(10))
    print(f'\nShape: {df.shape}')
    print(f'\nSites: {df.site_id.unique()}')
    print(f'\nAnomaly check:')
    print(df[df.level_m > 2.0][['site_id', 'timestamp', 'level_m', 'pump_status']])