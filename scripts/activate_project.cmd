@echo off

call D:\BigData\NYC-Taxi-Sparkling-Water\.venv\Scripts\activate.bat

set "JAVA_HOME=D:\BigData\Tools\Java\jdk-17"
set "SPARK_HOME=D:\BigData\Tools\Spark\spark-3.5.0-bin-hadoop3"
set "HADOOP_HOME=%SPARK_HOME%\hadoop"
set "HADOOP_CONF_DIR=%HADOOP_HOME%\conf"

set "TEMP=D:\BigData\Temp"
set "TMP=D:\BigData\Temp"
set "PIP_CACHE_DIR=D:\BigData\pip-cache"
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

set "JAVA_TOOL_OPTIONS=-Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8 --add-exports=java.base/sun.net.www.protocol.http=ALL-UNNAMED --add-exports=java.base/sun.net.www.protocol.https=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"

set "PATH=%JAVA_HOME%\bin;%HADOOP_HOME%\bin;%SPARK_HOME%\bin;%PATH%"

cd /d D:\BigData\NYC-Taxi-Sparkling-Water

echo ============================================
echo NYC TAXI SPARKLING WATER ENVIRONMENT READY
echo ============================================
echo JAVA_HOME=%JAVA_HOME%
echo SPARK_HOME=%SPARK_HOME%
echo HADOOP_HOME=%HADOOP_HOME%
python --version
java -version
