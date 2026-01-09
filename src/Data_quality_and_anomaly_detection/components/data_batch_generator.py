import os
from src.Data_quality_and_anomaly_detection.components.data_preprocessing import DataPreprocessing
from src.Data_quality_and_anomaly_detection.logger import logging
from src.Data_quality_and_anomaly_detection.exception import MycustomException
from src.Data_quality_and_anomaly_detection.utils.common import create_artifacts_dir
import pandas as pd

class DataBatchGenerator:
    """
    This class is responsible for creating different type of batches, like missing value batch, unsual/invalid
    value batch , duplicate-heavy value batch, time-based anomalies, mixed schema issue
    """
    def __init__(self,raw_data_path: str= "raw_csv",
                 bad_data_path: str = "bad_batches"):
        self.raw_data_path = raw_data_path
        self.bad_data_path = bad_data_path
        # self.datapreprocessing = DataPreprocessing(self.raw_data_path)
        self.bad_batch_path = create_artifacts_dir(data_path=self.bad_data_path)
    
    def get_dataframe(self):
        datapreprocessor = DataPreprocessing(self.raw_data_path)
        csv_path = datapreprocessor.convert_excel_to_csv()
        df = pd.read_csv(csv_path)
        return df

    def create_missing_value_batch(self, 
                                   batch_name: str = "missing_value_batch.csv",
                                   min_null_column: int = 1,
                                   batch_size: int = 5000

                                   )-> str:
        """
        Create a batch with higher missing values
        """

        try:
            batch_file_path = os.path.join(self.bad_batch_path,batch_name)
            if os.path.exists(batch_file_path) and os.path.getsize(batch_file_path)>0:
                logging.info(f"{batch_name} already exist at location {batch_file_path}")
                return batch_file_path
            
            logging.info("Starting missing value batch creation")
            df = self.get_dataframe()

            # count null per rows 
            df["null_count"]  = df.isnull().sum(axis=1)

            # selec rows with atleast missing nulls
            missing_df = df[df['null_count']>=min_null_column]

            if missing_df.empty:
                logging.info("NO misssing value present in current dataset")
            
            logging.info(f"Found {missing_df.shape[0]} rows with >= {min_null_column} null values")

            # Focus on known problematic column
            missing_df.loc[:,'CustomerID'] = None
            missing_df.loc[:,'Description'] = None

            #Sample fixed batch size
            missing_batch  =  missing_df.sample( n = min(batch_size,missing_df.shape[0]),
                                                random_state= 42)
            missing_batch.drop(columns=["null_count"], inplace= True)

            missing_batch.to_csv(batch_file_path,index=False)

            logging.info(f"Missing value batch is created at: {batch_file_path}, successfully!")

            return batch_file_path
        
        except Exception as e:
            raise  MycustomException(e)

    def create_invalid_value_batch(self,
                                   batch_name : str = 'invalid_value_batch.csv',
                                   batch_size : int = 5000) -> str:
        """
        Just like missing value batch creating, another
          batch created which will have invalid/unusual values in quantity and price column
        """
        try:
            batch_file_path = os.path.join(self.bad_batch_path,batch_name)
            if os.path.exists(batch_file_path) and os.path.getsize(batch_file_path)>0:
                logging.info(f"{batch_name} already exist at location {batch_file_path}")
                return batch_file_path
            
            df = self.get_dataframe()

            logging.info(f"called get dataframe fumnction and saved dataframe in df with shape: {df.shape}")

            logging.info("Starting batch creation of invalid values")

             
            invalid_values_df = df.sample(
                                        n = min(batch_size,len(df)),
                                        random_state= 42
                                        ).copy()
            
            #Inject invalid values
            # Rule 1 unit price>0
            invalid_values_df.loc[:,'UnitPrice'] = -1

            # Rule 2: Quantity extremely high
            invalid_values_df.loc[invalid_values_df.index[:1000],'Quantity'] = 99999

            df.to_csv(batch_file_path,index=False)

            logging.info(f"find invalid value batch craeted successfully at {batch_file_path}")
            return batch_file_path
        except Exception as e:
            raise MycustomException(e)
    
    def create_duplicate_value_batch(self,
                                     batch_name : str = "duplicate_heavy_batch.csv",
                                     batch_size: int = 2000,
                                     duplication_factor: int= 3
                                    ) -> str:
        """
        Creating a batch with higher number of duplicate values 
        """
        try:
            batch_file_path = os.path.join(self.bad_batch_path,batch_name)
            if os.path.exists(batch_file_path) and os.path.getsize(batch_file_path)>0:
                logging.inof(f"{batch_name} exist at location {batch_file_path}")
                return batch_file_path

            df = self.get_dataframe()
            logging.info("Read data frame")
            
            # Sample base row from df
            base_df = df.sample(
                n = min(batch_size,len(df)),
                random_state= 42
            )

            # Duplicate data into base df
            duplicate_df = pd.concat(
                [base_df]*duplication_factor,
                ignore_index= True
            )

            # shuffle rows

            duplicate_df = duplicate_df.sample(
                frac = 1,
                random_state=42
            ).reset_index(drop=True)
            duplicate_df.to_csv(batch_file_path,index = False)

            logging.info(f"Successfully created a batch with high number of duplicate values at location:{batch_file_path}")

            return batch_file_path
        
        except Exception as e:
            raise MycustomException(e)

    def create_time_based_anomaly_batch(self,
                                        batch_name: str = "time_based_anomaly_batch.csv",
                                        start_date: str = '2011-11-01',
                                        end_date: str = '2011-11-02')-> str:
        """
        Creating time based anomalies batch of one day
        """
        try:
            batch_file_path = os.path.join(self.bad_batch_path, batch_name)
            if os.path.exists(batch_file_path) and os.path.getsize(batch_file_path):
                logging.info(f"{batch_name} already exists at location {batch_file_path}")
                return batch_file_path
            
            logging.info("Read data frame")
            df = self.get_dataframe()
            df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

            time_base_df = df[(df["InvoiceDate"]>=start_date) &
                              (df['InvoiceDate']<=end_date)].copy()
            if time_base_df.empty:
                raise MycustomException(f"There is no data entries from {start_date} to {end_date}")

            #Inject anomaly, pricing failure
            time_base_df.loc[:,'UnitPrice'] = 0
            time_base_df.to_csv(batch_file_path, index = False)

            logging.info(
                f"Time-based anomaly batch created at {batch_file_path} "
                f"for window {start_date} to {end_date}"
            )

            return batch_file_path
        except Exception as e:
            raise MycustomException(e)
        
    def create_missing_schema_batch(self,
                                    batch_name: str = "missing_schema_batch.csv",
                                    column_to_drop: list = ['CustomerID']
                                    )-> str:
        """
        Create a missing schema batch in which a column will be missing

        """
        try:
            batch_file_path = os.path.join(self.bad_batch_path, batch_name)
            if os.path.exists(batch_file_path) and os.path.getsize(batch_file_path):
                logging.info(f"{batch_name} already exists at location {batch_file_path}")
                return batch_file_path
            logging.info("Reading dataframe")
            df = self.get_dataframe()

            for col in column_to_drop:
                if col not in df.columns:
                    raise MycustomException(f"{col} not found in dataset")
            # Drop a critical column from dataset
            missing_schema_df = df.drop(columns=column_to_drop)
            missing_schema_df.to_csv(batch_file_path)
            logging.info(
                f"Missing schema batch creation completed at {batch_file_path}"
                f"with dropped columns {column_to_drop}"
                f"and size of batch is: {missing_schema_df.shape}"
            )
            return batch_file_path
        except Exception as e:
            raise MycustomException(e)            
        
if __name__ =="__main__":

    obj = DataBatchGenerator()
    obj.create_missing_schema_batch()

        

