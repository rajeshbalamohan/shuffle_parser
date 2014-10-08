PIG_HOME=/grid/4/home/rajesh/tez-autobuild/pig-champlain/pig/
javac -cp $HADOOP_JARS:$PIG_HOME/*: org/pig/udf/*.java
javac -cp $HADOOP_JARS:$PIG_HOME/*: org/pig/storage/*.java
rm -rf udf.jar
jar -cvf udf.jar ./org


hadoop dfs -rmr -skipTrash /user/rajesh/pig/test/

$PIG_HOME/bin/pig -x tez -param_file params.txt -f parse_with_tfilestorage.pig
