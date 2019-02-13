# trace-data
Analyzes network trace data, shows top-n visitors and websites. 

## Requirements:
Write a program in Scala or Java that:
- Downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz 
- Determines the top-n most frequent visitors and urls for each day of the trace.  
- Package the application in a docker container.


## 
Include: 
- Logging
- Unit tests
- Documentation


## Executing Program in Docker 
```bash
sudo docker run --rm -it -p 4040:4040 \
-v /tmp:/tmp -v \
$(pwd)/trace-data.py:/trace-data.py \
gettyimages/spark bin/spark-submit \
/trace-data.py 2
```

## Inputs 



## Outputs
`/tmp/trace-data/*.log`