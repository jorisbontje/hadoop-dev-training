#!/bin/sh
export HADOOP_CLASSPATH=sssp.jar 
hadoop fs -text $1

