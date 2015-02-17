#!/usr/bin/env bash

DateToProcess=$(date --date="1 days ago" +%Y-%m-%d)

echo "Processing... "${DateToProcess}

echo "Cleaning new data."
pig -f cleaner.pig -param INPF=${DateToProcess} 

echo "Grouping ... "


pig -f grouper.pig -param INPF=${DateToProcess} 
