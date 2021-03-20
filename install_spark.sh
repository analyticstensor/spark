#!/bin/bash

# Install Spark. Need to run the scripte as  root user.

install_dir="/usr/local/apache-spark"
spark_download_url="https://www-us.apache.org/dist/spark/spark-3.0.0-preview/spark-3.0.0-preview-bin-hadoop3.2.tgz"
mkdir -p $install_dir
cd $install_dir
wget $spark_download_url
spark_home=`ls -1`
tar -zxvf $spark_home
rm -rf *.tgz
# rename spark home dir


