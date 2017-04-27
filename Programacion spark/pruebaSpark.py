# Databricks notebook source
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

rdd = sc.textFile("/../../generated.json")
#rdd = rdd.map(lambda line: (line.split(',')[0], line.split(',')[1]))
#schema = StructType([StructField(str(i), StringType(), True) for i in range(32)])
df = rdd.toDF()
#spark = SparkSession.builder.master('local[*]').appName('My App').config('spark.sql.warehouse.dir', 'file:///C:\Users\WhiteWolf\Desktop\generated.json').getOrCreate()
#df = spark.read.format('json').option('header', 'true').load('spark.sql.warehouse.dir', schema=schema) 

# COMMAND ----------

import csv
rdd2 = sc.textFile("2017-04-24_nontao.csv")
rdd2 = rdd2.mapPartitions(lambda x: csv.reader(x))
print rdd2

# COMMAND ----------

pruebas = rdd2.map(lambda line : line.split(","))
pruebas2 = pruebas.map(lambda p: (p[0], p[1].strip()))

# COMMAND ----------

sc = new SparkContext(args(0), "Csv loading example")
sqlContext = new org.apache.spark.sql.SQLContext(sc)
df = sqlContext.load("com.databricks.spark.csv", Map("path" -> args(1),"header"->"true"))
df.printSchema()

# COMMAND ----------

spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate
df = spark.read.format("com.databricks.spark.csv").option("header", "true")
headers.option("mode","DROPMALFORMED").load("csv/file/path"); #//.csv("csv/file/path") 
df.show()

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark import SQLContext
sqlContext = SQLContext(sc)

Employee_rdd = sc.textFile("\..\2017-04-24_nontao.csv").map(lambda line: line.split(","))

Employee_df = Employee_rdd.toDF(['Machine','Calendar','Time'])

Employee_df.show()

# COMMAND ----------

from pyspark import SparkContext 
from pyspark.sql import SQLContext 
import pandas as pd 
sqlc = SQLContext(sc) 
df = pd.read_csv("nontao.csv") 
sdf = sqlc.createDataFrame(df)

# COMMAND ----------

from pyspark.sql import SQLContext

df = sc.parallelize("generated2.json")
#df = SparkSession.read.json(df)
df = sqlContext.read.json(df)
print df.printSchema()

# COMMAND ----------

from pyspark.sql import SQLContext

df = sc.parallelize("generated.json")
df = sqlContext.read.json(df)
df.show()

# COMMAND ----------

df = spark.read.format('json').load('Usuarios/usuario/Escritorio/generated.json')
#df = spark.read.json("/Users/usuario/Desktop/generated.json")

# COMMAND ----------

people = sqlContext.read.json("file:///c:/Users/usuario/Desktop/generated.json")
people.printSchema()
people.registerTempTable("people")

# COMMAND ----------

js = [{'rating': 1, 'recipe_id': 8798, 'user_id': 2108},{'rating': 1, 'recipe_id': 8798, 'user_id': 2108},{'rating': 1, 'recipe_id': 8798, 'user_id': 2108}]
ratings_RDD = sc.parallelize(js)
ratings = ratings_RDD.map(lambda row:(Rating(int(row['user_id']),int(row['recipe_id']),float(row['rating']))))

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
df = spark.read.json("C:\Users\WhiteWolf\Desktop\generated.json")
