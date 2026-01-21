import os
from src.Data_quality_and_anomaly_detection.components.data_batch_generator import DataBatchGenerator
from src.Data_quality_and_anomaly_detection.logger import logging
from src.Data_quality_and_anomaly_detection.exception import MycustomException
import pandas as pd

class DataValidation:
    """This class will be responsible for validating the data"""

    def __init__(self,expected_schema):
        self.expected_schema = expected_schema
     
    def validate_schema(self,df:pd.DataFrame):
        try:
            logging.info("Started validating missing schema batch")
            schema_col = set(df.columns)
            expected_col = set(self.expected_schema.keys())

            missing_col = expected_col-schema_col
            extra_col = schema_col-expected_col

            dtype_issue= {}

            for col,expected_dtype in self.expected_schema.items():
                if col in df.columns:
                    if df[col].dtype.name != expected_dtype:
                        dtype_issue[col] = {
                            "expected":expected_dtype,
                            "actual":df[col].dtype.name
                        }
            logging.info(
                f"mising col info: {missing_col}",
                f"extra col info: {extra_col}",
                f"dtype mismatch info: {dtype_issue}"
            )
           
            return {
                "missing col": list(missing_col),
                "extra col": list(extra_col),
                "dtype issue" : dtype_issue
            }
            
        except Exception as e:
            raise MycustomException(e)
        
    def check_missing_value(self, df: pd.DataFrame, threshold: float = 0.05):
        try:
            logging.info("Started missing vlaue validation")
            total_rows = len(df)
            print(f"total rows: {total_rows}")
            print(f"Missing col: {df.isnull().sum()}")
            missing_report = {}
            for col in df.columns:
                missing_count = df[col].isnull().sum()
                missing_per = missing_count/total_rows 

                if missing_per>threshold:
                    missing_report[col] = {
                        "missing count": missing_count,
                        "missing_per": missing_per
                    }

            if missing_report:
                logging.info(f"Missing value check FAILED: {missing_report}")
                return {
                    "status": "Failed",
                    "missing report": missing_report
                }
            logging.info("Missing value check PASSED")

            return {
                "status": "PASSED",
                "missing report": {}
            }
        except Exception as e:
            raise (MycustomException(e))
       
    def check_duplicates(self, 
                         df: pd.DataFrame, 
                         subset_cols =None, 
                         threshold  = 0.05):
        try:
            logging.info("Starting validation of check duplicates")

            if subset_cols is None:
                subset_cols = ["InvoiceNo", "StockCode", "CustomerID"]

            total_rows = len(df)

            duplicate_mask = df.duplicated(subset=subset_cols, keep="first")
            duplicate_count = duplicate_mask.sum()
            duplicate_pct = duplicate_count / total_rows
            logging.info(
                f"Duplicate rows: {duplicate_count} "
                f"({round(duplicate_pct, 3) * 100}%) "
                f"based on columns {subset_cols}"
            )

            if duplicate_pct>threshold:
                return {
                    "status": "FAILED",
                    "duplicate_count": int(duplicate_count),
                    "duplicate_percentage": round(duplicate_pct, 3),
                    "duplicate_keys": subset_cols
                }

            return {
            "status": "PASSED",
            "duplicate_count": int(duplicate_count),
            "duplicate_percentage": round(duplicate_pct, 3)
                }

        except Exception as e:
            raise MycustomException(e)
        
    def check_invalid_values(self, df: pd.DataFrame, threshold: float= 0.01):
        try:
            logging.info("Checking invalid values")
            rules = {
                'Quantity': lambda x: x<0,
                'UnitPrice': lambda x: (x<0)| (x>=99999)
            }

            invalid_report = {}
            total_rows = len(df)

            for col, rule in rules.items():
                if col in df.columns:
                    invalid_cnt = rule(df[col]).sum()
                    invalid_pct = invalid_cnt/total_rows
                    # print(f"{col}:{invalid_pct}")
                    
                    if invalid_pct>threshold:
                        invalid_report[col] = {
                            "invalid count": int(invalid_cnt),
                            "invalid percentage": round(invalid_pct,3)
                        }
            
            if invalid_report:
                return {
                    "status": "Failed",
                    "Invalid columns": invalid_report
                }
            return {
                "status": "Passed",
                "Invalid columns":{}
            }

        except Exception as e:
            raise MycustomException(e)
        
    def check_timebased_anomaly(self,df: pd.DataFrame, threshold: float = 0.1):
        try:
            logging.info("Starting the validation of time based anomaly!!!!!")
            total_rows = len(df)

            
        except Exception as e:
            raise MycustomException(e)


if __name__ == "__main__":
    try:
        bacthgenerator = DataBatchGenerator()
        # missing_schema_path = bacthgenerator.create_missing_schema_batch()
        # missing_df = pd.read_csv(missing_schema_path, engine = 'python')
        # logging.info(f"Read missing df and it size is : {missing_df}")

        EXPECTED_SCHEMA = {
        "InvoiceNo": "object",
        "StockCode": "object",
        "CustomerID": "int64",
        "Description": "object",
        "Quantity": "int64",
        "InvoiceDate": "object",
        "UnitPrice": "float64",
        "Country": "object"
        }

        # data_validation = DataValidation(expected_schema=EXPECTED_SCHEMA)
        # schema_report = data_validation.validate_schema(df = missing_df) 
        # print("Validation schema report")
        # print(schema_report)

        # Validation missing value batch

        # missing_value_file = bacthgenerator.create_missing_value_batch()
        # df = pd.read_csv(missing_value_file)
        # logging.info("Read dataframe of missing value schema")
        data_validation = DataValidation(expected_schema=EXPECTED_SCHEMA)
        # missing_report = data_validation.check_missing_value(df = df)
        # print(missing_report)
        


        invalid_value_file = bacthgenerator.create_invalid_value_batch()
        df = pd.read_csv(invalid_value_file)
        invalid_report = data_validation.check_invalid_values(df = df)
        print(invalid_report)

    except Exception as e:
        raise MycustomException(e)

