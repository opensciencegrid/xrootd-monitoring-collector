
DateToProcess=$(date --date="1 days ago" +%Y-%m-%d)
echo "Processing "${DateToProcess}

pig -f summaryFilter.pig -param INPF=${DateToProcess} 
