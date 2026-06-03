from pathlib import Path

# Root of the project — works on any machine, any OS
BASE_DIR = Path(__file__).resolve().parents[1]

# Data folders
RAW_DIR       = BASE_DIR / 'data' / 'raw'
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
LOG_DIR       = BASE_DIR / 'logs'

# Create folders if they do not exist yet
for folder in [RAW_DIR, PROCESSED_DIR, LOG_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# API config
EA_API_BASE_URL = 'https://environment.data.gov.uk/flood-monitoring/id/stations'
EA_API_LIMIT    = 500

# Pipeline config
SCADA_SITES     = ['PS001', 'PS002', 'PS003', 'SR001', 'SR002']
SCADA_PERIODS   = 96   # one reading every 15 minutes = 96 per day