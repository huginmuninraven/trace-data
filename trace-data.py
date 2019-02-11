from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import argparse
import logging
import re
import urllib.request
import time


###########
# Logging #
###########

# Set up logging, to a file with the current timestamp.
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    filename='trace-data-' + time.strftime('%Y_%m_%d_%H_%M' + '.log'),
                    level=logging.INFO)

# Log4j logging example
# sparkLogger = sc._jvm.org.apache.log4j
# log = sparkLogger.logManager.getLogger(__name__)
# log.info("pyspark script logger initialized")

###############
# Spark Setup #
###############

conf = SparkConf()
conf.setAppName("TraceDataParser")
sc = SparkContext(conf=conf)

####################
# Argument Parsing #
####################

# Argument parsing: Declare a positional argument, input is an integer.
parser = argparse.ArgumentParser()
parser.add_argument("topn", type=int, help="Enter an integer between 1 and 10 to calculate the top visitors and websites")
args = parser.parse_args()

print(args.topn)


'''
from urllib.request import Request, urlopen
from urllib.error import URLError
req = Request(ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz)
try:
    response = urlopen(req)
except URLError as e:
    if hasattr(e, 'reason'):
        print('We failed to reach a server.')
        print('Reason: ', e.reason)
    elif hasattr(e, 'code'):
        print('The server couldn\'t fulfill the request.')
        print('Error code: ', e.code)
else:
    # everything is fine
'''


# Parses each line of trace data out into fields, handling groupings.
def parsedata(traceinput):
    # Following regex is purposely greedy, in order to handle double quotes in url field
    parsedline = re.findall('\[[^\]]*\]|".*"|\S+', traceinput)
    try:
        if len(parsedline) is not 7:
            print("line wrong length" + traceinput)
            return []
        else:
            # Format date time object
            dateclean = datetime.datetime.strptime(parsedline[3], '[%d/%b/%Y:%H:%M:%S %z]')
            parsedline[3] = str(dateclean.date())
            return parsedline
    except Exception as e:
        print(e)
        print(parsedline[3])
        print(traceinput)
        return []

# https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/dealing_with_bad_data.html
def try_correct_json(json_string):
  try:
    # First check if the json is okay.
    json.loads(json_string)
    return [json_string]
  except ValueError:
    try:
      # If not, try correcting it by adding a ending brace.
      try_to_correct_json = json_string + "}"
      json.loads(try_to_correct_json)
      return [try_to_correct_json]
    except ValueError:
      # The malformed json input can't be recovered, drop this input.
      return []

################
# Main Program #
################

# Downloads file locally to the /tmp directory, since we aren't using S3 or HDFS
local_filename, headers = urllib.request.urlretrieve('ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz', '/tmp/NASA_access_log_Jul95.gz')

# Open File as an RDD, since we are dealing with a teext file
traceFile = sc.textFile("/tmp/NASA_access_log_Jul95.gz")
rddcount = traceFile.count()
logging.info("Raw Line Count: " + str(rddcount))

# Transform data into a dataframe
cleanedData = traceFile.map(parsedata)
parsedcount = cleanedData.count()
logging.info("Parsed Line Count: " + str(parsedcount))

logging.info("Percentage of Lines Parsed: " + str(parsedcount/rddcount))

# Rename the columns to something more human friendly
renamed = cleanedData.toDF("visitor", "uk1", "uk2", "date", "url", "httpcode", "bytes")

countedvisitors = renamed.groupBy(['date', 'visitor']).count()
countedurls = renamed.groupBy(['date', 'url']).count()


# Use a window to organize the data by date and visitors
visitorwindow = Window.partitionBy('date').orderBy('date', countedvisitors['count'].desc())
topvisitors = countedvisitors.select('*', rank().over(window).alias('rank')).filter(col('rank') <=5).orderBy('date', 'rank')

# Use a window to organize the data by date and urls
urlwindow = Window.partitionBy('date').orderBy('date', countedurls['count'].desc())
topurls = countedurls.select('*', rank().over(urlwindow).alias('rank')).filter(col('rank') <=5).orderBy('date','rank')

# Stop Spark session
spark.stop()



# Create a temporary table for sql queries
# cleanedData.createOrReplaceTempView("tracetable")

# sqlDF = spark.sql("SELECT _4, COUNT(_4) FROM tracetable GROUP BY _4 ORDER BY _4")

# spark.sql("SELECT _4, COUNT(_4) as cnt FROM tracetable GROUP BY _1, _4 ORDER BY cnt DESC LIMIT 5").take(5)