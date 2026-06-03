import logging
from pathlib import Path

# Create logs folder if it does not exist
LOG_DIR = Path(__file__).resolve().parents[1] / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# Configure logging — writes to a file AND shows in the terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'pipeline.log'),
        logging.StreamHandler()
    ]
)

# Create a named logger for the whole pipeline
logger = logging.getLogger('utility_telemetry_pipeline')