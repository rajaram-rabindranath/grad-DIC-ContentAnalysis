#!/bin/sh
cp /SkyDrive/svn/Twitter/dist/twitter-hashtag.jar /home/hduser/
rm -r ~/graph*
hdfs dfs -rm /input/*
hdfs dfs -copyFromLocal /SkyDrive/svn/Twitter/input/input-graph.txt /input
hadoop jar twitter-hashtag.jar shortestPath.ShortestPathDriver 1
#hdfs dfs -rm /input/*
#hdfs dfs -cp /output/part* /input
#hadoop jar twitter-hashtag.jar sortbyvalue.SortDriver 1
hdfs dfs -copyToLocal /output/graph* ~/
#mv ~/part* ~/wordcount.txt
cp -r ~/graph* /SkyDrive/svn/Twitter/output/bina/
