import argparse
import datetime
import logging
import re
import urllib.request
import time

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

###############
# Spark Setup #
###############
conf = SparkConf()
conf.setAppName("TraceDataParser")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('abc').getOrCreate()
# Suppress Spark INFO logging after startup
sc.setLogLevel("WARN")


###########
# Logging #
###########
# Set up logging, to a file with the current timestamp.
# In a distributed cluster,  leverage the Spark History Server
# and send the data out for visualization to an external store (Elasticsearch/Grafana/Prometheus)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    filename='/opt/trace-data-' + time.strftime('%Y_%m_%d_%H_%M' + '.log'),
                    level=logging.INFO)



# Argument Parsing
def parse_args():
    # Declare a positional argument, input is an integer.
    parser = argparse.ArgumentParser()
    parser.add_argument("topn", type=int, choices=range(1,11), help="Enter an integer between 1 and 10 to calculate the top visitors and websites")
    args = parser.parse_args()
    logging.info("Calculating " + str(args.topn) + " top visitors and urls." )
    return args.topn


# Parses each line of trace data out into fields, handling groupings.
def parse_data(trace_input):
    try:
    # Following regex is purposely greedy, in order to handle double quotes in url field
        parsed_line = re.findall('\[[^\]]*\]|".*"|\S+', trace_input)
        if len(parsed_line) is not 7:
            logging.warn("Bad input line: " + trace_input)
            return []
        else:
            # Format date time object, another alternative would be to use the to_date call in Spark
            date_clean = datetime.datetime.strptime(parsed_line[3], '[%d/%b/%Y:%H:%M:%S %z]')
            parsed_line[3] = str(date_clean.date())
            return parsed_line
    except Exception as e:
        logging.error(e, trace_input)
        return

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


################
# Main Program #
################

def main():
    topn = parse_args()

    # Downloads file locally to the /tmp directory, since we aren't using S3 or HDFS
    urllib.request.urlretrieve('ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz', '/tmp/NASA_access_log_Jul95.gz')

    # Open File as an RDD, since we are dealing with a text file
    traceFile = sc.textFile("/tmp/NASA_access_log_Jul95.gz")
    rdd_count = traceFile.count()
    logging.info("Raw Line Count: " + str(rdd_count))

    # Parses RDD into a list and caches the RDD for future use.
    cleanedData = traceFile.map(parse_data).cache()

    # Filter out lines != length of 7 and empty rows
    filtered_data = cleanedData.filter(lambda x: len(x) is 7).filter(lambda x: x is not None).toDF()

    # Rename the columns to something more human friendly
    renamed = filtered_data.toDF("visitor", "uk1", "uk2", "date", "url", "httpcode", "bytes")

    # Calculate and log percentage of lines that were transformed successfully
    df_count = renamed.count()
    logging.info("Parsed Line Count: " + str(df_count))
    logging.info("Percentage of Lines Parsed: " + str(df_count/rdd_count))

    # Group data by the visitors / urls, so we can run a window over them
    counted_visitors = renamed.groupBy(['date', 'visitor']).count()
    counted_urls = renamed.groupBy(['date', 'url']).count()

    # Use a window to organize the data by date and visitors
    visitor_window = Window.partitionBy('date').orderBy('date', counted_visitors['count'].desc())
    top_visitors = counted_visitors.select('*', rank().over(visitor_window).alias('rank')).filter(col('rank') <=topn).orderBy('date', 'rank')

    # Repartition to 1, because we know the dataset is trivial in size
    # Saving to csv for readability, would usually use parquet
    top_visitors.repartition(1).write.csv('/opt/topvisitors')
    logging.info("Completed visitor CSV file.")

    # Use a window to organize the data by date and urls
    url_window = Window.partitionBy('date').orderBy('date', counted_urls['count'].desc())
    top_urls = counted_urls.select('*', rank().over(url_window).alias('rank')).filter(col('rank') <=topn).orderBy('date', 'rank')

    # Repartition to 1, because we know the dataset is trivial in size
    # Saving to csv for readability, would usually use parquet
    top_urls.repartition(1).write.csv('/opt/topurls')
    logging.info("Completed URL CSV file.")

    # Stop Spark session
    sc.stop()

if __name__ == "__main__":
    main()