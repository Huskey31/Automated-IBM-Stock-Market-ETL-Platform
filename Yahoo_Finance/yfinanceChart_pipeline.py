import pandas as pd
import sqlalchemy
import matplotlib.pyplot as plt
import logging
from datetime import datetime
import os

logging.basicConfig(
    filename= r"C:\Users\khany\OneDrive\Desktop\Stuff\Richfield studies\DE_projects\PipeLines\Yahoo_Finance\yfinance.log",
    level=logging.INFO,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode='a'
)

try:
    logging.info("Starting the YFinance Chart Pipeline")
    df = pd.read_sql(
        "SELECT * FROM yfinance",
        sqlalchemy.create_engine("postgresql+psycopg2://#####:######@Localhost:5432/ETL_Database")
    )

    plt.figure(figsize=(10,6))
    # Scatter each price column against Volume
    plt.scatter(df["Volume"], df["Open"], color='blue', alpha=0.5, label='Open')
    plt.scatter(df["Volume"], df["High"], color='green', alpha=0.5, label='High')
    plt.scatter(df["Volume"], df["Low"], color='red', alpha=0.5, label='Low')
    plt.scatter(df["Volume"], df["Close"], color='purple', alpha=0.5, label='Close')

    plt.title("YFinance Stock Data Scatter Plot")
    plt.xlabel("Volume")
    plt.ylabel("Stock Prices")
    plt.grid(True)
    plt.legend()

    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_path = r"C:\Users\khany\OneDrive\Desktop\Stuff\Richfield studies\DE_projects\PipeLines\Yahoo_Finance\Graphs"
    os.makedirs(save_path, exist_ok=True)
    filename = os.path.join(save_path, f"yfinance_{timestamp}.png") 

    plt.savefig(filename, dpi=300)
    logging.info("YFinance Chart Pipeline completed successfully")

except Exception as e:
    logging.error(f"Error as occurred {e}")