from pyspark.sql import SparkSession

# Path to the JDBC driver JAR file
jdbc_driver_path = "D:/JDBC/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar"

# Create a SparkSession and specify the JDBC driver path
spark = SparkSession.builder \
    .appName("MySQL to PySpark DataFrame") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

# Define the JDBC URL for the MySQL connection
jdbc_url = "jdbc:mysql://localhost:3306/challenge1"

# Connection properties, including user credentials
connection_properties = {
    "user": "root",
    "password": "Anonymous@100",
    "driver": "com.mysql.cj.jdbc.Driver"  # MySQL Connector driver
}

# Specify the table name you want to read from MySQL
table_name = "challenge1"

# Load the table into a DataFrame
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

# Show the data from the DataFrame (you can adjust the number of rows to display)
df.show()

# Stop the Spark session when done
spark.stop()

