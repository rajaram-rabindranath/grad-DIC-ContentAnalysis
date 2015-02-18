#!/bin/sh
cp /SkyDrive/svn/Twitter/dist/twitter-hashtag.jar /home/hduser/
rm ~/part*
hdfs dfs -rm /input/*
hdfs dfs -copyFromLocal /SkyDrive/svn/Twitter/input/dataPull_clean/* /input
hadoop jar twitter-hashtag.jar relative_tagcoocc.stripes.StripesRelativeHashTagCoOccDriver 1
#hdfs dfs -rm /input/*
#hdfs dfs -cp /output/part* /input
#hadoop jar twitter-hashtag.jar sortbyvalue.SortDriver 1
hdfs dfs -copyToLocal /output/part* ~/
mv  ~/part* ~/stripes_relative.txt
cp ~/stripes_relative.txt /SkyDrive/svn/Twitter/output
