import os
from src.Data_quality_and_anomaly_detection.logger import logging
from src.Data_quality_and_anomaly_detection.exception import MycustomException
from src.Data_quality_and_anomaly_detection.utils.common import create_artifacts_dir
import subprocess
from datetime import datetime


class DataIngestion():
    """
    This module will download the online retail datatset and save it at raw folder 
    which is under artifacts folder
    """
    def __init__(self):
        self.datapath = create_artifacts_dir(data_path = "raw")
        self.kaggle_dataset = "jihyeseo/online-retail-data-set-from-uci-ml-repo"
        
    
    def initiate_ingestion(self)->str:

        try:
            dataset_name = "Online Retail"
            file_name = f"{dataset_name}.xlsx"
            file_path = os.path.join(self.datapath,file_name)

            if os.path.exists(file_path) and os.path.getsize(file_path)>0:
                logging.info("Dataset already exist!!!!!!!!!!!!")
                return file_path
            
            logging.info(f"Created {self.datapath} and about to run kaggle command")

            # Build kaggle command line
            cmd = [
            "kaggle","datasets","download",
            "-d", self.kaggle_dataset,
            "-p", self.datapath,
            "--unzip"
            ]

                       
            logging.info(f"started dataset downloading with {cmd} from kaggle at {datetime.now()}")

            # run the command 
            subprocess.run(cmd, check = True)
            logging.info("Kaggle dataset downloaded successfully!!")



            if not os.path.exists(file_path) or os.path.getsize(file_path)==0:
                raise MycustomException("Downloaded file empty or missing")


            logging.info(f"Dataset downloaded successfully at {self.datapath} and saved as {file_name}")
            return file_path
        except Exception as e:
            raise MycustomException(e)

    # def get_dataset_file(self,extensions = (".csv",".xlsx"))-> str:
    #     try:
    #         data_path = self.initiate_ingestion()

    #         for file in os.listdir(data_path):
    #             if file.lower().endwith(extensions):
    #                 logging("file path exist with required extension")
    #                 return os.path.join(data_path,file)
    #     except Exception as e:
    #         raise(MycustomException(e))
        

# if __name__ == "__main__":
#     obj = DataIngestion()
#     obj.initiate_ingestion()
