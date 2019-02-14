# trace-data
## Summary: 
Analyzes network trace data, shows top-n visitors and websites. 


## Requirements:
Write a program that:
- Downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz 
- Determines the top-n most frequent visitors and urls for each day of the trace.  
- Package the application in a docker container.


## Program Outline

### Prerequisites: 
The system running this program should have: 
- Docker Installed
- Internet Connection is assumed

### Execution Instructions
Step 1. Clone this repo. 

Step 2. Program execution example
Execute the following command from the directory that contains trace-data.py

```bash
sudo docker run --rm -it -p 4040:4040 \
-v $(pwd):/opt \
-v $(pwd)/tracedata.py:/tracedata.py \
gettyimages/spark bin/spark-submit \
/tracedata.py 7
```

### Program Inputs 
1. Command Line Argument:  
This program takes in an integer from 1-10 used to return the top visitors and websites. 

2. Trace file from NASA:  
The program downloads this from the internet. 

### Program Outputs 
All program outputs are saved to the directory that the program was executed in.  

Logging:  
`trace-data/*.log`  

Top N Visitors:  
- A CSV file that contains top-n visitors from each day.   
    Format: `Date|Visitor|Count|Rank`

Top N Urls:  
- A CSV file that contains the top-n URls visited each day. 


## Testing
Spark jobs should be broken down into individual functions, so they can be atomically tested.  
This testing framework is modeled after the following example:  
https://engblog.nextdoor.com/unit-testing-apache-spark-with-py-test-3b8970dc013b

There is probably a better way to do this in Spark2 with Scala.

### Prerequisites: 
- Spark installed on the system
- pytest installed
  `conda install pytest`  
- pytest-spark installed https://github.com/malexer/pytest-spark/blob/master/README.rst  
  `pip install pytest-spark`  
- Configure pytest.ini with path to pyspark  
    `/opt/spark-2.4...`

### Running Tests
`cd` to this repo, that has been cloned.  
`/opt/miniconda/3.7/bin/python3.7 -m pytest`


## Future Work: 

### Cluster mode 
Run this on a spark cluster, using the docker-compose.yml, or possibly on Mesos/Kubernetes

#### Prerequisites: 
- Install docker-compose

#### Instructions:  
`docker-compose up`  
`bin/run-example --num-executors 2 --executor-memory 2G SparkPi 10000`  



### Complete test coverage  
- Refactor the code into functions   
- Write test cases for each one  
- Use Coverage to look for missing tests  
