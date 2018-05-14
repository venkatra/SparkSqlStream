#!/usr/bin/env bash


LIB_DIR=/Users/d3vl0p3r/Dev/lib

export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
export MAVEN_HOME=$LIB_DIR/apache-maven-3.5.0
export HADOOP_HOME=$LIB_DIR/hadoop-2.7.0
export SBT_HOME=$LIB_DIR/sbt
export SPARK_HOME=$LIB_DIR/spark-2.3.0-bin-hadoop2.7

PATH=$PATH:$SBT_HOME/bin
PATH=$PATH:$MAVEN_HOME/bin
PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
PATH=$PATH:$SPARK_HOME/bin

export PS1="\W>"

#cleaning up directories
rm -rf target/feed_checkpoint target/rsvp_feed

APP_CLASS="ca.effpro.explore.spark.meetup.RandomAlphaStreamAppMain"
#APP_CLASS="ca.effpro.explore.spark.meetup.MeetupRSVPStreamAppMain"

spark-submit --class ${APP_CLASS} \
--master local[2]  \
target/scala-2.11/jars/SparkSqlStream-1.0-SNAPSHOT.jar
