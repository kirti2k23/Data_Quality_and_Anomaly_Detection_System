import os
from src.Data_quality_and_anomaly_detection.logger import logging
from src.Data_quality_and_anomaly_detection.exception import MycustomException
from src.Data_quality_and_anomaly_detection.components.data_ingestion import DataIngestion
from src.Data_quality_and_anomaly_detection.utils.common import create_artifacts_dir
import pandas as pd


class DataPreprocessing:
    def __init__(self, output_path: str):
        
        self.folder_path = output_path
        

    def convert_excel_to_csv(self)-> str:
        try:
           
            output_path = create_artifacts_dir( data_path=self.folder_path)

            csv_path = os.path.join(output_path,"Online_retail.csv")

            if os.path.exists(csv_path) and os.path.getsize(csv_path)>0:
                logging.info(f"Dataset in csv format already exist at file path {csv_path}")
                return csv_path
            
            ingetsion = DataIngestion()
            excel_file = ingetsion.initiate_ingestion()

            df = pd.read_excel(excel_file)

            logging.info(f"Excel loaded with shape, {df.shape}")
            
            

            logging.info("converting excel data into csv data")
            df.to_csv(csv_path, index= False)

            logging.info(f"CSV file saved at: {csv_path}")

            return csv_path

            
        except Exception as e:
            raise(MycustomException(e))
        
# if __name__ == "__main__":
#     obj = DataPreprocessing(output_path="raw_csv")
#     obj.convert_excel_to_csv()