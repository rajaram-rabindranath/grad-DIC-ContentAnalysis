folder structure:
->code
  ->Analysis (Code for Twitter Analysis)
    ->scripts (contains scripts to run code)
    ->conf (has config data)
  ->Visualization (Code for Visualization)
->output
->input
->report
-> <jar> file for your convinience

Instructions About important folders:

Note_1:The "scripts" folder contains the scripts to execute the code for the corresponding problem.
	> Please do remember to make changes to in/out paths in each .sh file

Note_2:The contents of the "conf" folder will also need to be changed

Note_3:The "input" folder has input data and the fields information

Instructions on how to run the code if you do not want to run  the scripts in "scritps":

KMEANS:
hadoop jar twitter-hashtag.jar kmeans.KMeansDriver <#_K>
	e.g. hadoop jar <jar_name_prj2> kmeans.KMeansDriver 2
WordCount:
hadoop jar twitter-hashtag.jar wordcount.WordCountDriver 1

AuthorCount:
hadoop jar twitter-hashtag.jar authorcount.AuthorCountDriver 1

HashTagCount:
hadoop jar twitter-hashtag.jar hashtagcount.HashTagCountDriver 1
hadoop jar twitter-hashtag.jar relative_hashtagcount.RelativeHashTagCountDriver 1


ShortestPath:
hadoop jar twitter-hashtag.jar shortestPath.ShortestPathDriver 1
hadoop jar twitter-hashtag.jar shortestPath.ShortestPathDriver 1

Pairs:
hadoop jar twitter-hashtag.jar tagcoocc.pairs.PairsHashTagCoOccDriver 1
hadoop jar twitter-hashtag.jar relative_tagcoocc.pairs.PairsRelativeHashTagCoOccDriver 1

Stripes:
hadoop jar twitter-hashtag.jar tagcoocc.stripes.StripesHashTagCoOccDriver 1
hadoop jar twitter-hashtag.jar relative_tagcoocc.stripes.StripesRelativeHashTagCoOccDriver 1

UserTags:
hadoop jar twitter-hashtag.jar userreferencecount.UserReferenceCountDriver 1

