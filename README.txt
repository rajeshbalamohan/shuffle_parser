yarn logs -applicationId appId > SOME_HDFS_LOCATION

populate params.txt

execute "run.sh"

Plotting:
========

-- Get the data to a local file and get it in your local machine
hadoop dfs -cat /user/rajesh/pig/test/results/end_to_end_result.csv/* > end_to_end_result.csv

type "gnuplot" in a mac terminal (hoping you have already installed)

set xlabel 'source-machine'
set ylabel 'destination-machine'
set zlabel 'Shuffle data BYTES per thread per attempt'
set datafile separator " "
set pm3d
set palette
set hidden3d
set dgrid3d 50,50 qnorm 2
set datafile separator ' '
splot '/Users/rbalamohan/Downloads/end_to_end_result.csv' using 4:5:6 with l



For per attempt timings
=======================
hadoop dfs -cat /user/rajesh/pig/test/results/attempt_timings.csv/* > attempt_timings.csv

set xlabel 'source-machine'
set ylabel 'destination-machine'
set zlabel 'Shuffle data BYTES per thread per attempt'
set datafile separator " "
set pm3d
set palette
set hidden3d
set dgrid3d 50,50 qnorm 2
set datafile separator ' '
splot '/Users/rbalamohan/Downloads/attempt_timings.csv' using 4:5:7 with l


For data transfer:
==================
set xlabel 'source-machine'
set ylabel 'destination-machine'
set zlabel 'Shuffle data BYTES per thread per attempt'
set datafile separator " "
set pm3d
set palette
set hidden3d
set dgrid3d 50,50 qnorm 2
set datafile separator ' '
splot '/Users/rbalamohan/Downloads/attempt_timings.csv' using 4:5:9 with l
