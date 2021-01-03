# TLQ - Transform Load Query Data Processing Pipeline - A Serverless Cloud Native Application

The AWS Lambda TLQ ( Transform, Load, Query) data processing pipeline is a serverless application and is a variant of the Extract-Transform-Load (ETL) pipeline.
```sh
- [X] The T service performs Extraction and Transformation of the input data
- [X] The L service loads the transformed data into the Aurora Serverless RDS database
- [X] The Q service performs queries on the data which is loaded in the database.
```

A case study is conducted on performance and throughput variance for two of the service compositions.
- [X] Service composition #1 - T, L and Q service is deployed as independent Lambda functions and each lambda function is invoked sequentially.
- [X] Service composition #2 - A switch board architecture is implemented in which all the three services are combined into a single deployment package and is implemented as a single lambda function. Individual calls are made to perform the T, L & Q services but each function call goes to the same lambda function thereby minimizing the cold start times for the L & Q services.

The performance metrics are collected using the **FAAS Runner tool** that helps in conducting the experiment for multiple runs and pipelining the functions. The results of this performance analysis would help developers to choose the best design that would improve performance, throughput and helps in the reduction of the overall cost of the deployment of microservices.
