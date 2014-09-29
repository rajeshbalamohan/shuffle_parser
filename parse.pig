register '/grid/4/home/rajesh/tez-autobuild/pig-champlain/pig/datafu-1.2.0.jar';
register '/grid/4/home/rajesh/tez-autobuild/pig-champlain/pig/piggybank.jar';
register '/grid/4/home/rajesh/tez-autobuild/pig-champlain/udf/udf.jar';


-- TODO:  This can easily be optimized using SPLIT in Pig, so that we read the source only once.  But as of now, its fine.
DEFINE parseFetcherData() RETURNS void {
 -- Generate data sets by parsing logs (Generate rate.csv and store it for referencing it later)
	raw = LOAD '$INPUT_LOGS' USING PigStorage() as (line:chararray);
	completedLines = FOREACH raw GENERATE org.pig.udf.ProcessCompletedLines(line, '$MACHINE_MAPPING_FILE_IN_HDFS', 'completed') as (completed:chararray);
	completedLinesFiltered = FILTER completedLines BY completed is not null;
	STORE completedLinesFiltered INTO '$ATTEMPT_INFO';

	-- Generate srcToAttempt Information (store it for referencing it later)
	raw1= LOAD '$INPUT_LOGS' USING PigStorage() as (line:chararray);
  raw = FILTER raw BY line MATCHES '.*for url.*';
  httpLines = FOREACH raw GENERATE org.pig.udf.ProcessCompletedLines(line, '$MACHINE_MAPPING_FILE_IN_HDFS', 'http') as (http:chararray);
  httpLinesFiltered = FILTER httpLines BY http is not null;
  STORE httpLinesFiltered INTO '$SRC_TO_ATTEMPT_INFO';
}

-- Generate end to end shuffle time (i.e connect time till the last attempt fetch per source->destination)
DEFINE generateEndToEndShuffleTime() RETURNS void {
	raw = LOAD '$INPUT_LOGS' USING PigStorage() as (line:chararray);
	completedLines = FOREACH raw GENERATE org.pig.udf.ProcessCompletedLines(line, '$MACHINE_MAPPING_FILE_IN_HDFS', 'EndToEnd') as (completed:chararray);
	completedLinesFiltered = FILTER completedLines BY completed is not null;
	STORE completedLinesFiltered INTO '$END_TO_END_TIMINGS';
}

-- At a per thread and per attempt level, generate the source to destination mapping
DEFINE generateSourceToDestinationMappingPerAttempt() RETURNS void {
	rate1 = LOAD '$ATTEMPT_INFO' using PigStorage(',') as (millis:long, vertex:chararray, threadId:chararray, src:chararray, attempt:chararray, timeTaken:chararray, transferRate:chararray, compressedSize:chararray);
	rate = FOREACH rate1 GENERATE millis, vertex, threadId,src,attempt, timeTaken, transferRate, compressedSize;
	srcToAttempt1 = LOAD '$SRC_TO_ATTEMPT_INFO' using PigStorage(',') as (millis:long, vertex:chararray, threadId:chararray, src:chararray, dest:chararray, query:chararray);
	srcToAttempt = FOREACH srcToAttempt1 GENERATE millis, vertex, threadId, src, dest, query;
	j = JOIN rate BY (vertex, threadId, src),  srcToAttempt BY (vertex, threadId,src) PARALLEL 20;
	result = FOREACH j GENERATE rate::millis, rate::vertex, rate::threadId, rate::src, srcToAttempt::dest, rate::attempt, rate::timeTaken, rate::transferRate, rate::compressedSize;
	STORE result INTO '$ATTEMPT_TIMINGS' USING PigStorage(',');
}


-- clean up the directories (more like initial cleanup)
-- fs -rmr /user/rajesh/pig/data/query92_10tb_25_sep/rate.csv;
-- fs -rmr /user/rajesh/pig/data/query92_10tb_25_sep/srcToAttempt.csv;
-- fs -rmr /user/rajesh/pig/data/query92_10tb_25_sep/results.csv;
-- fs -rmr /user/rajesh/pig/data/query92_10tb_25_sep/end_to_end_result.csv;

-- parse the fetcher logs
parseFetcherData();
exec;

-- Generate end to end shuffle time (i.e connect time till the last attempt fetch per source->destination)
generateEndToEndShuffleTime();
exec;

-- Generate per attempt per thread information
generateSourceToDestinationMappingPerAttempt();
exec;

