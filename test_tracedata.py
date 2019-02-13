
# Parses each line of trace data out into fields, handling groupings.
def parse_data(trace_input):
    try:
        # Following regex is purposely greedy, in order to handle double quotes in url field
        parsed_line = re.findall('\[[^\]]*\]|".*"|\S+', trace_input)
        print(parsed_line)
        if len(parsed_line) is not 7:
            #logging.warning("Bad input line: " + trace_input)
            return []
        else:
            # Format date time object, another alternative would be to use the to_date call in Spark
            date_clean = datetime.datetime.strptime(parsed_line[3], '[%d/%b/%Y:%H:%M:%S %z]')
            parsed_line[3] = str(date_clean.date())
            return parsed_line
    except Exception as e:
        #logging.error(e, trace_input)
        return []




def test_parse_data(spark_context):
    """ Test tracedata parsing
    Args:
        spark_context: test fixture SparkContext
    """
    test_input = ['199.0.2.27 - - [28/Jul/1995:13:32:23 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 200 5866']

    input_rdd = spark_context.parallelize(test_input)
    results = parse_data(input_rdd)
    print(results)

    expected_results = {["199.0.2.27", "-", "-", "1995/07/28", "GET /images/ksclogo-medium.gif HTTP/1.0", 200, 5866]}
    assert results == expected_results
    #assert results in