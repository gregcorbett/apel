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
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql import functions as func

print "Starting"
print datetime.now()

conf = SparkConf().setAppName('PySpark Summariser').setMaster('local[4]')

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
streamingContext = StreamingContext(sc,1)

tds = streamingContext.textFileStream("/tmp/datastream").map(lambda line: line.split(","))

df_running = None

def summariser(time, rdd):

    print("========= %s =========" % time)

    try: 
        df = rdd.toDF(['SiteID','VOID','GlobalUserNameID','VOGroupID','VORoleID','EndYear','EndMonth','InfrastructureType','SubmitHostID','ServiceLevelType','ServiceLevel','NodeCount','Processors'])

        window_result = df.groupBy('SiteID', 'VOID', 'GlobalUserNameID', 'VOGroupID', 'VORoleID', 'EndYear', 'EndMonth', 'InfrastructureType', 'SubmitHostID', 'ServiceLevelType', 'ServiceLevel', 'NodeCount', 'Processors').count()

        global df_running
        if df_running is None:
            df_running = window_result
        else:
            df_running = df_running.unionAll(window_result)

        # this allows us to pass back the re grouped dataframe in a format that can be unioned with the next window
        df_running = df_running.groupBy('SiteID', 'VOID', 'GlobalUserNameID', 'VOGroupID', 'VORoleID', 'EndYear', 'EndMonth', 'InfrastructureType', 'SubmitHostID', 'ServiceLevelType', 'ServiceLevel', 'NodeCount', 'Processors').agg(func.sum('count').alias('count'))

 
        df_running.orderBy('EndMonth', 'GlobalUserNameID','count').show(14289842)

    except ValueError:
        print "No new data."

#result = df.groupBy('SiteID', 'VOID', 'GlobalUserNameID', 'VOGroupID', 'VORoleID', 'EndYear', 'EndMonth', 'InfrastructureType', 'SubmitHostID', 'ServiceLevelType', 'ServiceLevel', 'NodeCount', 'Processors').count().orderBy('EndMonth', 'GlobalUserNameID','count').show(14289842)

tds.foreachRDD(summariser)

streamingContext.start()
streamingContext.awaitTermination()

#df.printSchema()

#result = sqlContext.sql('SELECT UpdateTime, EndYear, EndMonth, count(*) as NJobs FROM JobRecords GROUP by 2,3').collect()

#result.show()

#for row in result:
#    print ('| %s | %s | %s | %s |' % row.UpdateTime, row.EndYear, row.EndMonth, row.NJobs)
