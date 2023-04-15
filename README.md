# ecc-assignment2

## Introduction
This project aims to perform analysis on the  Parking_Violations_Issued_-_Fiscal_Year_2023 and NBA shot logs data using PySpark on a 3-node cluster. 

## Pre-requites
Spark,
Java,
Python

## Directory Structure
Go to /usr/local/spark folder 

## Setup
Use command->  
./sbin/start-master.sh – This will start the master at the port 8080 and will setup our cluster. 
./bin/spark-class org.apache.spark.deploy.worker.Worker spark://master.js2local:7077 - This will start the worker, do this twice to make two slaves/worker.

## How to run the python programs
bin/spark-submit --master spark://149.165.153.171:7077 assignment2/ques*_part*.py
#### Instead of * there will be number e.g. ques1_part1.py etc. 

## Where to look for output
./assignment2/outputs/parking_violations_outputs/
This will contain specific folder for the output. And inside that folder you can see the part-0000 file for output. 
cat part-0000 will print the output. 

