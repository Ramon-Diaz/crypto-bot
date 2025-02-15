from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySpark MySQL Example") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# JDBC connection properties
url = "jdbc:mysql://mysql:3306/crypto-data"
properties = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load data from MySQL into a Spark DataFrame
df = spark.read.jdbc(url=url, table="your_table_name", properties=properties)

# Perform operations on the DataFrame
df.show()
