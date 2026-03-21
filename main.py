from utils.logger import write_log
from pipelines.load_data import load_data
from pipelines.transform import transform_data
from pipelines.ingestion import run_ingestion
from datetime import datetime
import time


logger = write_log()

logger.info(f"Pipeline execution started at: {datetime.now()}")
start_time = time.time()

if __name__ == "__main__":
    run_ingestion()
    transform_data()
    load_data()


end_time = time.time()
logger.info(f"Pipeline execution completed at: {datetime.now()}")
logger.info(
    f"Time taken to execute the pipeline is: {end_time - start_time:.2f} seconds")
