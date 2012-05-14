#!/bin/sh
file=$(ls dist/* | tail -n 1)
echo "Using $(basename $file)" 
export HADOOP_CLASSPATH=".:$file"
