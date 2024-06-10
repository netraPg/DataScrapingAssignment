import time
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Set up the WebDriver (make sure to specify the path to your ChromeDriver if it's not in your PATH)
driver = webdriver.Chrome()

# Open the webpage
driver.get("https://www.speedtest.net/global-index")

# Wait for the page to load completely
time.sleep(5)  # Adjust this as necessary
driver.find_element(By.XPATH, "//button[@class='btn']").click()
time.sleep(5)  # Adjust this as necessary

# Extract Mobile Data table
mobile_data_table = driver.find_element(By.XPATH, "//*[@id='column-mobileMedian']//table")
mobile_rows = mobile_data_table.find_elements(By.TAG_NAME, 'tr')

mobile_data = []
for row in mobile_rows[1:]:  # Skip the header row
    cells = row.find_elements(By.TAG_NAME, 'td')
    if len(cells) == 4:
        mobile_data.append([cell.text for cell in cells])

mobile_df = pd.DataFrame(mobile_data, columns=['Rank','Latency (ms)', 'Country', 'Speed (Mbps)'])

# Extract Broadband Data table
broadband_data_table = driver.find_element(By.XPATH, "//*[@id='column-fixedMedian']//table")
broadband_rows = broadband_data_table.find_elements(By.TAG_NAME, 'tr')

broadband_data = []
for row in broadband_rows[1:]:  # Skip the header row
    cells = row.find_elements(By.TAG_NAME, 'td')
    if len(cells) == 4:
        broadband_data.append([cell.text for cell in cells])

broadband_df = pd.DataFrame(broadband_data, columns=['Rank','Latency (ms)', 'Country', 'Speed (Mbps)'])

# Print the dataframes
print("Mobile Data:")
print(mobile_df)

print("\nBroadband Data:")
print(broadband_df)

# Optionally, save to CSV files
mobile_df.to_csv('mobile_data.csv')
broadband_df.to_csv('broadband_data.csv')

# Close the WebDriver
driver.quit()

# Create MySQL engine
# Set up logging
logging.basicConfig(level=logging.INFO)

# Create MySQL engine
engine = create_engine('mysql+mysqlconnector://root:2001@localhost:3306/pythondb')

Base = declarative_base()


# Define schema and tables
class MobileData(Base):
    __tablename__ = 'mobile_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    rank = Column(String(50))
    latency_ms = Column(String(50))
    country = Column(String(100))
    speed_mbps = Column(Float)



class BroadbandData(Base):
    __tablename__ = 'broadband_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    rank = Column(String(50))
    latency_ms = Column(String(50))
    country = Column(String(100))
    speed_mbps = Column(Float)


# Create tables
Base.metadata.create_all(engine)

# Load CSV files into pandas DataFrames
mobile_df = pd.read_csv('mobile_data.csv')
broadband_df = pd.read_csv('broadband_data.csv')

# Create session
Session = sessionmaker(bind=engine)
session = Session()

try:
    # Insert mobile data
    for _, row in mobile_df.iterrows():
        try:
            record = MobileData(
                rank=row['Rank'],
                latency_ms=row['Latency (ms)'],
                country=row['Country'],
                speed_mbps=float(row['Speed (Mbps)'])

            )
            session.add(record)
        except ValueError as e:
            logging.error(f"Error converting 'Speed (Mbps)' or 'Latency (ms)' to float: {e}")
            continue

    # Insert broadband data
    for _, row in broadband_df.iterrows():
        try:
            record = BroadbandData(
                rank=row['Rank'],
                latency_ms=row['Latency (ms)'],
                country=row['Country'],
                speed_mbps=float(row['Speed (Mbps)'])

            )
            session.add(record)
        except ValueError as e:
            logging.error(f"Error converting 'Speed (Mbps)' or 'Latency (ms)' to float: {e}")
            continue

    # Commit the session
    session.commit()

    logging.info("Data has been saved to the MySQL database with the specified schema.")
except Exception as e:
    logging.error(f"An error occurred: {e}")
finally:
    # Close the session
    session.close()
