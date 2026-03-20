from pipelines.ingestion import ingestion
from pipelines.load_data import load_data
from pipelines.transform import transform_data
from datetime import datetime
import time
from utils.logger import write_log

logger = write_log()

start_time = time.time()
logger.info(f"Pipeline execution started at: {datetime.now()}")

ingestion()
transform_data()
load_data()

end_time = time.time()
logger.info(f"Pipeline execution completed at: {datetime.now()}")
logger.info(
    f"Time taken to execute the pipeline is: {end_time-start_time:.2f} seconds")
