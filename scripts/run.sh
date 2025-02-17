#!/bin/bash

cd /home/sz/bce_embedding
LOGFILE="log/script.log"
LOG_LEVEL="debug"
echo "Starting new session..." > $LOGFILE
echo "Starting main_api_bce.py in the background..." >> $LOGFILE
python3 main_api_bce.py >> $LOGFILE 2>&1 &
sleep 20

cd /home/sz/qdrant_vector_database
LOGFILE="log/script.log"
LOG_LEVEL="debug"
echo "Starting new session..." > $LOGFILE
echo "Starting main_api_qdrant.py in the background..." >> $LOGFILE
python3 main_api_qdrant.py >> $LOGFILE 2>&1 &
sleep 20

cd /home/sz/shangzhi_bi
LOGFILE="log/script.log"
LOG_LEVEL="debug"
echo "Starting main_api_shangzhi_bi.py in the background..." >> $LOGFILE
python3 main_api_qdrant_fuyiyuan.py >> $LOGFILE 2>&1 &

sleep 999999d





