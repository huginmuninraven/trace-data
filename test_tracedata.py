import re
import datetime
import pytest

# Parses each line of trace data out into fields, handling groupings.
def parse_data(trace_input):
    import re
    import datetime
    try:
        #print(trace_input)
        # Following regex is purposely greedy, in order to handle double quotes in url field
        parsed_line = re.findall('\[[^\]]*\]|".*"|\S+', trace_input)
        #print(parsed_line)
        if len(parsed_line) is not 7:
            #logging.warning("Bad input line: " + trace_input)
            return []
        else:
            # Format date time object, another alternative would be to use the to_date call in Spark
            date_clean = datetime.datetime.strptime(parsed_line[3], '[%d/%b/%Y:%H:%M:%S %z]')
            parsed_line[3] = str(date_clean.date())
            return parsed_line
    except Exception as e:
        return []



# Test doesn't pass, possibly an issue with imports
def test_parse_data(spark_context):
    """ Test tracedata parsing
    Args:
        spark_context: test fixture SparkContext
    """
    test_input = ['199.0.2.27 - - [28/Jul/1995:13:32:23 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 200 5866']

    input_rdd = spark_context.parallelize(test_input)
    results = parse_data(input_rdd)

    expected_results = ['199.0.2.27', '-', '-', '1995-07-28', '"GET /images/ksclogo-medium.gif HTTP/1.0"', '200', '5866']
    assert results == expected_results




# Super simple test to validate that this framework, does, in fact, work.
# test_tracedata.py::test_do_word_counts PASSED

def do_line_counts(lines):
    """ count of words in an rdd of lines """

    counts = lines.count()
    results = counts
    return results


def test_do_line_counts(spark_context):
    """ test word couting
    Args:
        spark_context: test fixture SparkContext
    """
    test_input = [
        ' hello spark ',
        ' hello again spark spark'
    ]

    input_rdd = spark_context.parallelize(test_input, 1)
    results = do_line_counts(input_rdd)

    expected_results = 2
    assert results == expected_results