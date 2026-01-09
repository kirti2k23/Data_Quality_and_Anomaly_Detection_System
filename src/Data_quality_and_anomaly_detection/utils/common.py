import os
from src.Data_quality_and_anomaly_detection.logger import logging
from src.Data_quality_and_anomaly_detection.exception import MycustomException




def create_artifacts_dir(data_path: str)-> str:
    try:
        folder = "artifacts"
        artifacts_path = os.path.join(folder,data_path)
        if os.path.exists(artifacts_path):
            logging.info(f"{artifacts_path} already exist")
            return artifacts_path
        os.makedirs(artifacts_path,exist_ok=True)
        logging.info(f"{folder}/{data_path} is created successfully")
    except Exception as e:
        raise MycustomException(e)
    return artifacts_path