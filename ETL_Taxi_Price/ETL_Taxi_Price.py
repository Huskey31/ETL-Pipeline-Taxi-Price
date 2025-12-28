import pandas as pd
import logging
import random
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    filename= r"C:\Users\khany\OneDrive\Desktop\Stuff\Richfield studies\DE_projects\PipeLines\ETL_Taxi_Price\ETL_Taxi_Price.Log",
    level=logging.INFO,
    format='%(asctime)s-%(levelname)s-%(message)s',
    filemode='a'
)

try:
    df = pd.read_csv(r"C:\Users\khany\OneDrive\Desktop\Stuff\Richfield studies\DE_projects\PipeLines\ETL_Taxi_Price\archive\taxi_trip_pricing.csv")
    logging.info("Extracting From CSV File has completed")
    #Changing columns to numeric
    logging.info("Changing columns to numeric")
    df["Trip_Distance_km"] = pd.to_numeric(df["Trip_Distance_km"])
    df["Passenger_Count"] = pd.to_numeric(df["Passenger_Count"])
    df["Base_Fare"] = pd.to_numeric(df["Base_Fare"])
    df["Per_Km_Rate"] = pd.to_numeric(df["Per_Km_Rate"])
    df["Per_Minute_Rate"] = pd.to_numeric(df["Per_Minute_Rate"])
    df["Trip_Price"] = pd.to_numeric(df["Trip_Price"])
    logging.info("Changing columns to numeric has completed")


    #Handiling missing Values
    logging.info("Handling missing Values")
    df["Trip_Distance_km"] = df["Trip_Distance_km"].fillna(df["Trip_Distance_km"].mean()).round(2)
    df["Time_of_Day"] = df["Time_of_Day"].fillna("Afternoon")
    df["Day_of_Week"] = df["Day_of_Week"].fillna("Weekday")
    df["Passenger_Count"] = df["Passenger_Count"].fillna(df["Passenger_Count"].mean()).round(2)
    df["Traffic_Conditions"] = df["Traffic_Conditions"].fillna("Medium")
    df["Weather"] = df["Weather"].fillna("Clear")
    df["Base_Fare"] = df["Base_Fare"].fillna(df["Base_Fare"].mean()).round(2)
    df["Per_Km_Rate"] = df["Per_Km_Rate"].fillna(df["Per_Km_Rate"].mean()).round(2)
    df["Per_Minute_Rate"] = df["Per_Minute_Rate"].fillna(df["Per_Minute_Rate"].mean()).round(2)
    df["Trip_Duration_Minutes"] = df["Trip_Duration_Minutes"].fillna(df["Trip_Duration_Minutes"].mean()).round(2)
    df["Trip_Price"] = df["Trip_Price"].fillna(df["Trip_Price"].mean()).round(2)
    logging.info("Handling of missing values has completed")

    #Loading Data to Database
    logging.info("loading data to database started")
    engine = create_engine("postgresql+psycopg2://######:######@Localhost:5432/ETL_Database") # code changed to hide creditials
    df.to_sql("Taxi_Trip_Pricing", engine, if_exists='replace',index=False)
    logging.info("Data Loading to database has completed")
except Exception as e:
    logging.error(f"Taxi Price ETL Error:{e}")