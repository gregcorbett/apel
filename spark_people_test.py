import os
import sys
from datetime import datetime

# Set up PySpark
os.environ['SPARK_HOME']="/spark"
SPARK_HOME=os.environ.get('SPARK_HOME',None)
#os.environ["SPARK_CLASSPATH"] = '/spark/bin/mysql-connector-java-5.1.40-bin.jar'
sys.path.insert(0, os.path.join(SPARK_HOME, "python", "lib"))
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))


from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


print "Starting"
print datetime.now()

conf = SparkConf().setAppName('PySpark People Agg').setMaster('local[1]')


sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

print "Reading Data"
print datetime.now()

df = sqlContext.read.format('jdbc').option('url','jdbc:mysql://localhost:3306/spark_dummy_test?user=root').option('dbtable','people').option("driver", 'com.mysql.jdbc.Driver').load()

print 'Printing'
print datetime.now()

df.printSchema()

print 'Agging'
result = df.groupBy('age').count()

print 'Print Again'
result.show()

print "Done"
print datetime.now()

#df.printSchema()

#result = sqlContext.sql('SELECT UpdateTime, EndYear, EndMonth, count(*) as NJobs FROM JobRecords GROUP by 2,3').collect()

#result.show()

