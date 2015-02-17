#!/usr/bin/env bash

DateToProcess=$(date --date="1 days ago" +%Y-%m-%d)

echo "Processing... "${DateToProcess}

echo "Cleaning new data."
pig -f cleaner.pig -param INPF=${DateToProcess} 

hdfs dfs -get Summary/Sorted/sorted.${DateToProcess}/part-r-00000

python binner.py

rm part-r-00000