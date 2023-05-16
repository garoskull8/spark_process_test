# -*- coding: utf-8 -*-
"""
Created on Mon May 15 08:09:23 2023

@author: edgar
"""
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, first, desc, to_date,date_format
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType
import os
import sys



def hired_employees(filename):
    
    os.environ["JAVA_HOME"] = 'C:/Program Files/Java/jdk1.8.0_171'
    os.environ["PYSPARK_SUBMIT_ARGS"]="--master local[2] pyspark-shell"
    url = 'mongodb://localhost:27017/test_enterprise.hired_employees'
    spark= SparkSession\
        .builder\
        .appName("spark_process_application")\
        .master("local[*]")\
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2') \
        .config('spark.jars','file:///C:/Users/edgar/Desktop/globant_proyect_tech_interview/spark_process/spark-avro_2.12-3.1.2.jar')\
        .config("spark.mongodb.read.connection.uri",url)\
        .config("spark.mongodb.write.connection.uri", url)\
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    hired_employees_schema= StructType()\
        .add("id",IntegerType(),False)\
        .add("name",StringType(),False)\
        .add("datetime",TimestampType(),False)\
        .add("department_id",IntegerType(),False)\
        .add("job_id",IntegerType(),False)    

    
    hired_df=spark\
        .read\
        .schema(hired_employees_schema)\
        .option("header","false")\
        .option("mode","DROPMALFORMED")\
        .csv("C:/Users/edgar/Desktop/globant_proyect_tech_interview/rest_api/globant-rest-api/uploads/in/staging/"+filename)

    hired_df.printSchema()
    hired_df.show()
    
    hired_df.write.format('avro').mode('overwrite')\
        .save('C:/Users/edgar/Desktop/globant_proyect_tech_interview/rest_api/globant-rest-api/uploads/raw/hired_employees')
    hired_df.write.format("mongodb").mode("overwrite").save()     
    spark.stop()
    
def departments(filename):
    
    os.environ["JAVA_HOME"] = 'C:/Program Files/Java/jdk1.8.0_171'
    os.environ["PYSPARK_SUBMIT_ARGS"]="--master local[2] pyspark-shell"
    url = 'mongodb://localhost:27017/test_enterprise.departments'
    spark= SparkSession\
        .builder\
        .appName("spark_process_application")\
        .master("local[*]")\
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2') \
        .config('spark.jars','file:///C:/Users/edgar/Desktop/globant_proyect_tech_interview/spark_process/spark-avro_2.12-3.1.2.jar')\
        .config("spark.mongodb.read.connection.uri",url)\
        .config("spark.mongodb.write.connection.uri", url)\
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    department_schema= StructType()\
        .add("id",IntegerType(),False)\
        .add("department",StringType(),False)


    
    department_df=spark\
        .read\
        .schema(department_schema)\
        .option("header","false")\
        .option("mode", "DROPMALFORMED")\
        .csv("C:/Users/edgar/Desktop/globant_proyect_tech_interview/rest_api/globant-rest-api/uploads/in/staging/"+filename)

    department_df.printSchema()
    department_df.show()
    
    department_df.write.format('avro').mode('overwrite')\
        .save('C:/Users/edgar/Desktop/globant_proyect_tech_interview/rest_api/globant-rest-api/uploads/raw/departments')
    department_df.write.format("mongodb").mode("overwrite").save()     
    spark.stop()
    
def jobs(filename):
    
    os.environ["JAVA_HOME"] = 'C:/Program Files/Java/jdk1.8.0_171'
    os.environ["PYSPARK_SUBMIT_ARGS"]="--master local[2] pyspark-shell"
    url = 'mongodb://localhost:27017/test_enterprise.jobs'
    spark= SparkSession\
        .builder\
        .appName("spark_process_application")\
        .master("local[*]")\
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2') \
        .config('spark.jars','file:///C:/Users/edgar/Desktop/globant_proyect_tech_interview/spark_process/spark-avro_2.12-3.1.2.jar')\
        .config("spark.mongodb.read.connection.uri",url)\
        .config("spark.mongodb.write.connection.uri", url)\
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    job_schema= StructType()\
        .add("id",IntegerType(),False)\
        .add("job",StringType(),False)


    
    job_df=spark\
        .read\
        .schema(job_schema)\
        .option("header","false")\
        .option("mode", "DROPMALFORMED")\
        .csv("C:/Users/edgar/Desktop/globant_proyect_tech_interview/rest_api/globant-rest-api/uploads/in/staging/"+filename)

    job_df.printSchema()
    job_df.show()
    
    job_df.write.format('avro').mode('overwrite')\
        .save('C:/Users/edgar/Desktop/globant_proyect_tech_interview/rest_api/globant-rest-api/uploads/raw/jobs')
    job_df.write.format("mongodb").mode("overwrite").save()     
    spark.stop()
    
def back_up():
    os.environ["JAVA_HOME"] = 'C:/Program Files/Java/jdk1.8.0_171'
    os.environ["PYSPARK_SUBMIT_ARGS"]="--master local[2] pyspark-shell"
    url = 'mongodb://localhost:27017/test_enterprise'
    spark= SparkSession\
        .builder\
        .appName("spark_process_application")\
        .master("local[*]")\
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2') \
        .config('spark.jars','file:///C:/Users/edgar/Desktop/globant_proyect_tech_interview/spark_process/spark-avro_2.12-3.1.2.jar')\
        .config("spark.mongodb.read.connection.uri",url)\
        .config("spark.mongodb.write.connection.uri", url)\
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    job_schema= StructType()\
        .add("id",IntegerType(),False)\
        .add("job",StringType(),False)
    
    department_schema= StructType()\
        .add("id",IntegerType(),False)\
        .add("department",StringType(),False)
    
    hired_employees_schema= StructType()\
        .add("id",IntegerType(),False)\
        .add("name",StringType(),False)\
        .add("datetime",TimestampType(),False)\
        .add("department_id",IntegerType(),False)\
        .add("job_id",IntegerType(),False)    

    job_df=spark\
        .read\
        .format("avro")\
        .schema(job_schema)\
        .option("mode", "DROPMALFORMED")\
        .load("C:/Users/edgar/Desktop/globant_proyect_tech_interview/rest_api/globant-rest-api/uploads/raw/jobs")
        
    department_df=spark\
        .read\
        .format("avro")\
        .schema(department_schema)\
        .option("mode", "DROPMALFORMED")\
        .load("C:/Users/edgar/Desktop/globant_proyect_tech_interview/rest_api/globant-rest-api/uploads/raw/departments")
        
    hired_employees_df=spark\
        .read\
        .format("avro")\
        .schema(hired_employees_schema)\
        .option("mode", "DROPMALFORMED")\
        .load("C:/Users/edgar/Desktop/globant_proyect_tech_interview/rest_api/globant-rest-api/uploads/raw/hired_employees")
        
    job_df.write.format("mongodb").option("collection", "jobs").mode("overwrite").save()   
    department_df.write.format("mongodb").option("collection", "departments").mode("overwrite").save()    
    hired_employees_df.write.format("mongodb").option("collection", "hired_employees").mode("overwrite").save()    
    spark.stop()
        
    
if __name__ == "__main__" :
    arg=sys.argv[1]
    print(arg)
    hired_employee="hired_employees"
    department="departments"
    job="jobs"
    backup="backup"
    
    if hired_employee in arg:
        hired_employees(arg)
    elif department in arg:
        departments(arg)
    elif job in arg:
        jobs(arg)
    elif backup in arg:
        back_up()
        
 