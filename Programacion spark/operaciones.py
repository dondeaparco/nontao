# Databricks notebook source
#en 'df' esta guardado el dataframe con todos los datos
#
#
#locZ = df.select(df['locZ'])
#locM = df.select(df['locM'])
#locU = df.select(df['locU'])
#distUZ = locU - locZ
#distUM = locU - locM
#
#
#
jsonStrings0 = ['{"zona":"A1","locZ": { "lat" : 3114 , "long" : 8160}, "maq": "001","locM": { "lat" : 31144 , "long" : 8440}, "userID" : "12345" , "locU": { "lat" : 3114 , "long" : 8160},"distUM": 0 , "distUZ": 0}']
jsonStrings1 = ['{"zona":"A1","locZ": { "lat" : 2114 , "long" : 8164}, "maq": "001","locM": { "lat" : 30014 , "long" : 816120}, "userID" : "12345" , "locU": { "lat" : 3412 , "long" : 8110},"distUM": 0 , "distUZ": 0}']
jsonStrings2 = ['{"zona":"A0","locZ": { "lat" : 5214 , "long" : 8190}, "maq": "001","locM": { "lat" : 311475 , "long" : 816122}, "userID" : "12345" , "locU": { "lat" : 31154 , "long" : 8460},"distUM": 0 , "distUZ": 0}']
otherPeopleRDD0 = sc.parallelize(jsonStrings0)
otherPeopleRDD1 = sc.parallelize(jsonStrings1)
otherPeopleRDD2 = sc.parallelize(jsonStrings2)
otherPeople0 = spark.read.json(otherPeopleRDD0)
otherPeople1 = spark.read.json(otherPeopleRDD1)
otherPeople2 = spark.read.json(otherPeopleRDD2)
otherPeople0.createOrReplaceTempView("people0")
otherPeople1.createOrReplaceTempView("people1")
otherPeople2.createOrReplaceTempView("people2")
#result = spark.sql("SELECT people0.maq \
#                   FROM people0 \
#                   INNER JOIN people0 ON people0.zona==people1.zona")
d1m = spark.sql("SELECT locZ.lat from people0").show()
d2m = spark.sql("SELECT locZ.long from people0").show()
d1u = spark.sql("SELECT locU.lat from people1").show()
d2u = spark.sql("SELECT locU.long from people1").show()
distlat = d1m-d1u
distlong = d2m-d2u
#result = spark.sql("select (distUM={distlat,distlong}) from people0")
result = spark.sql("INSERT INTO people1 [otherPeople0]people0 [(distUM={distlat,distlong})] select_statement")
#INSERT INTO [TABLE] [db_name.]table_name [PARTITION part_spec] select_statement
result.show()

# COMMAND ----------


