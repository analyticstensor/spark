# Load types
from pyspark.sql.types import *
from pyspark.sql import SparkSession

empSchema = StructType([
        StructField("emp_no", IntegerType(), False),
        StructField("birth_date", DateType(), False),
        StructField("FIRST_NAME", StringType(), False),
        StructField("LAST_NAME", StringType(), False),
        StructField("GENDER", StringType(), False),
        StructField("hire_Date", DateType(), False)
])

# Create data
empData = [[1, 1950-1-9, 'Jimmy', 'Doe', 'M', 1975-10-15],
           [2, 1952-8-15, 'Smith', 'Butler', 'F', 1972-10-15],
           [3, 1953-12-10, 'David', 'Jackson', 'M', 1980-10-15],
           [4, 1960-5-25, 'Jina', 'Unknown', 'F', 1990-10-15],
           [5, 1945-12-16, 'Smith', 'Unknown', 'F', 1965-10-15],
           [6, 1980-3-12, 'Jim', 'Unknown', 'NA', 2005-10-15],
           [7, 1981-8-23, 'Jimmy', 'Unknown', 'F', 2015-10-15],
           [8, 1975-7-24, 'Jimmy', 'Unknown', 'NA', 2019-10-15],
           [9, 1990-2-21, 'Jimmy', 'Unknown', 'F', 2018-10-15],
           [10, 1991-1-18, 'Jimmy', 'Unknown', 'NA', 2014-10-15]] 

# Create Sparksession
spark = SparkSession.builder.appName("Employee").getOrCreate()

# Create DataFrame from define schema and data
employee_df = spark.createDataFrame(empData, empSchema)

# print schema of DataFrame
print(employee_df.printtSchema())

# Show DataFrame
print("==========\nEmployee DataFrame\n=============\n")
employee_df.show()

# Error: Py4JError: org.apache.spark.api.python.PythonUtils.getEncryptionEnabled does not exist in the JVM
