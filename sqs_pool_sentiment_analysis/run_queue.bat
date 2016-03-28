echo off

echo "Running Assignment 2..."

set PY_SCRIPTS="C:\Users\rachmawati lim\Desktop\Spring 2016\COMS 6998 Cloud Computing and Big Data\Assignment 2\sqs_pool_sentiment_analysis"

rem This python example shows how to access the aws keys via environment variables
rem python %PY_SCRIPTS%\awskeyreadexample.py

python %PY_SCRIPTS%\sqsworkerpool.py
rem python %PY_SCRIPTS%\example.py

pause