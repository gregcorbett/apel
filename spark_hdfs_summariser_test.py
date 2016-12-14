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

conf = SparkConf().setAppName('PySpark Summariser').setMaster('local[4]')


sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

print "Reading Data"
print datetime.now()

rrd = sc.textFile("hdfs://localhost:9000/csvdumpFULL").map(lambda line: line.split(","))


df = rrd.toDF(['SiteID','VOID','GlobalUserNameID','VOGroupID','VORoleID','EndYear','EndMonth','InfrastructureType','SubmitHostID','ServiceLevelType','ServiceLevel','NodeCount','Processors'])

print "Filtering"
print datetime.now()

result = df.groupBy('SiteID', 'VOID', 'GlobalUserNameID', 'VOGroupID', 'VORoleID', 'EndYear', 'EndMonth', 'InfrastructureType', 'SubmitHostID', 'ServiceLevelType', 'ServiceLevel', 'NodeCount', 'Processors').count().orderBy('EndMonth', 'GlobalUserNameID','count').show(14289842)

print "Done"
print datetime.now()

#df.printSchema()

#result = sqlContext.sql('SELECT UpdateTime, EndYear, EndMonth, count(*) as NJobs FROM JobRecords GROUP by 2,3').collect()

#result.show()

#for row in result:
#    print ('| %s | %s | %s | %s |' % row.UpdateTime, row.EndYear, row.EndMonth, row.NJobs)
