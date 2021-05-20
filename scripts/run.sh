#!/bin/sh

# Entry point to run both the detailed and summary collectors in the same Docker container

DetailedLog=/var/log/xrootdcollector/DetailedCollector.log
SummaryLog=/var/log/xrootdcollector/SummaryCollector.log

mkdir -p /var/log/xrootdcollector

echo "Starting Detailed Collector and pushing log files to: ${DetailedLog}"

/app/DetailedCollector.py /configs/connection.conf 2>&1 | tee ${DetailedLog} &

echo "Starting Summary Collector and pushing log files to: ${SummaryLog}"

/app/SummaryCollector.py /configs/connection.conf 2>&1 | tee ${SummaryLog} &


# Based on the recommendation on Docker web-pages to identify commands which
# should be continuously running which we want to auto-restart should they break
# The difference comes from the actual 'command' 'ps c' which identifies a Python script
# Keep checking every 20s which is probably 'good enough'
while sleep 20; do

  ps acux | grep 'DetailedCollector' | grep -q -v grep
  PROCESS_1_STATUS=$?
  ps acux | grep 'SummaryCollector' | grep -q -v grep
  PROCESS_2_STATUS=$?
  
  if [ $PROCESS_1_STATUS -ne 0 ]; then
    echo "Detailed Collector died!"
    echo "Restarting Detailed Collector"
    /app/DetailedCollector.py /configs/connection.conf 2>&1 | tee ${DetailedLog} &
  fi

  if [ $PROCESS_2_STATUS -ne 0 ]; then
    echo "Summary Collector died!"
    echo "Restarting Summary Collector"
    /app/SummaryCollector.py /configs/connection.conf 2>&1 | tee ${SummaryLog} &
  fi

done

