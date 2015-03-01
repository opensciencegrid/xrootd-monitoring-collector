#!/usr/bin/env bash

cd /home/ivukotic/FAXtools/IlijaCollector

DateToProcess=$(date --date="1 hours ago" +%Y-%m-%d)
HourToProcess=$(date --date="1 hours ago" +%H)
echo "Processing... "${DateToProcess} ${HourToProcess}

echo "Cleaning new data."
pig -f cleaner.pig -param INPD=${DateToProcess} INPH=HourToProcess 

hdfs dfs -get Summary/Sorted/sorted.${DateToProcess}.${HourToProcess}/part-r-00000

python binner.py 

rm part-r-00000