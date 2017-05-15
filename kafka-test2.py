import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="kafkaTest")
#sc.setLogLevel("WARN")  
ssc = StreamingContext(sc,20)


kvs = KafkaUtils.createStream(ssc, "sandbox.hortonworks.com:2181", "spark_streaming", {"test": 1})
kvs.pprint()



ssc.start()

ssc.awaitTermination()


