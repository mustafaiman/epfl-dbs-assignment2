#!/usr/bin/env bash
GASPAR="iman"
SUBMISSION_DIR="submission/$GASPAR/exercise2"
SPARK_JAR="/home/mustafa/.m2/repository/org/apache/spark/spark-core_2.11/2.1.0/spark-core_2.11-2.1.0.jar"
APACHE_COMMONS_JAR="/home/mustafa/.m2/repository/org/apache/hadoop/hadoop-common/2.2.0/hadoop-common-2.2.0.jar"
SCALA_JAR="/home/mustafa/.m2/repository/org/scala-lang/scala-library/2.11.0/scala-library-2.11.0.jar"

CLASSPATH=$SPARK_JAR:$APACHE_COMMONS_JAR:$SCALA_JAR:.

javac $SPARK_DIR $SUBMISSION_DIR/task1/*.java
if [ $? -ne 0 ]
then
    exit 1
fi

javac -cp $CLASSPATH $SUBMISSION_DIR/task2/*.java
if [ $? -ne 0 ]
then
    exit 1
fi

javac -cp $CLASSPATH $SUBMISSION_DIR/task3/*.java
if [ $? -ne 0 ]
then
    exit 1
fi


javac -cp $CLASSPATH $SUBMISSION_DIR/task4/*.java
if [ $? -ne 0 ]
then
    exit 1
fi