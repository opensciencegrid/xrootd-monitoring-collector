#!/usr/bin/env bash

DateToProcess=$(date --date="1 days ago" +%Y-%m-%d)

echo "Processing... "${DateToProcess}

echo "Cleaning new data."
pig -f cleaner.pig -param INPF=${DateToProcess} 

echo "Grouping ... "

if hadoop fs -test –d Summary/Maxis ; then
    echo "Maxis directory exists"
else
    hadoop fs -mkdir Summary/Maxis
    echo “Creating Maxis directory”
fi

pig -f grouper.pig -param INPF=${DateToProcess} 
