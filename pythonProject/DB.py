import mysql.connector
from mysql.connector import Error

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="2001",
            database="speedtest_data"
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

# Replace the following placeholders with your MySQL credentials
connection = create_connection("localhost", "root", "2001", "speedtest_data")

# Example query
create_table_query = """
CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT, 
  name TEXT NOT NULL, 
  age INT, 
  gender TEXT, 
  nationality TEXT, 
  PRIMARY KEY (id)
) ENGINE = InnoDB
"""

execute_query(connection, create_table_query)