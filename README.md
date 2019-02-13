# trace-data
## Summary: 
Analyzes network trace data, shows top-n visitors and websites. 


## Requirements:
Write a program in Scala or Java that:
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
-v $(pwd)/trace-data.py:/trace-data.py \
gettyimages/spark bin/spark-submit \
/trace-data.py 7
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