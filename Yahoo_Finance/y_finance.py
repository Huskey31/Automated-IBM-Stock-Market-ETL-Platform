import yfinance as yf
import pandas as pd
import logging
import sqlalchemy

logging.basicConfig(
    filename= r"C:\Users\khany\OneDrive\Desktop\Stuff\Richfield studies\DE_projects\PipeLines\Yahoo_Finance\yfinance.log",
    level = logging.INFO,
    format = '%(asctime)s-%(levelname)s-%(message)s',
    filemode = 'a'
)

try:
    #Starting the yfinance data extraction process
    logging.info("Stating the yfinance data extraction process")
    df = yf.download("IBM", start="2025-11-01", end = None)
    logging.info("Data extraction completed successfully")

    #Starting the data transformation process
    logging.info("Starting the data transformation process")
    df.reset_index(inplace=True)
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df.columns = (["Date","Close","High","Low","Open","Volume"])
    df.drop_duplicates(subset=["Date"], inplace=True)

    #Starting the data loading process
    logging.info("Starting the data loading process")
    engine = sqlalchemy.create_engine("postgresql+psycopg2://######:######@Localhost:5432/ETL_Database") #changed code to hide password for github upload
    df.to_sql("yfinance", engine, if_exists="replace",index=False)
    logging.info("Data loading completed successfully")
except Exception as e:
    logging.error(f"An error occurred:{e}")

   