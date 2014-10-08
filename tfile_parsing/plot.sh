#!/bin/bash

function copy {
        hadoop dfs -cat /user/rajesh/pig/test/results/end_to_end_result.csv/* > end_to_end_result.csv
        hadoop dfs -cat /user/rajesh/pig/test/results/attempt_timings.csv/* > attempt_timings.csv
}


function plotEndToEndTiming {
        gnuplot <<- EOF
                set terminal svg size 1200,1200 fname 'Verdana' fsize 10
                set output 'plotEndToEndTiming.svg'
                set xlabel 'source-machine'
                set ylabel 'destination-machine'
                set zlabel 'Time (ms)'
                set pm3d
                set palette
                set hidden3d
                set dgrid3d 50,50
                set datafile separator ','
                splot 'end_to_end_result.csv' using 4:5:6 with l
EOF
}

function plotAttemptTimings {
        gnuplot <<- EOF
                set terminal svg size 1200,1200 fname 'Verdana' fsize 10
                set output 'plotAttemptTimings.svg'
                set xlabel 'source-machine'
                set ylabel 'destination-machine'
                set zlabel 'Shuffle per attempt Time (ms)'
                set pm3d
                set palette
                set hidden3d
                set dgrid3d 50,50
                set datafile separator ','
                splot 'attempt_timings.csv' using 4:5:7 with l
EOF
}

function plotShuffleBytesPerAttempt {
        gnuplot <<- EOF
                set terminal svg size 1200,1200 fname 'Verdana' fsize 10
                set output 'plotShuffleBytesPerAttempt.svg'
                set xlabel 'source-machine'
                set ylabel 'destination-machine'
                set zlabel 'Shuffle per attempt Bytes'
                set pm3d
                set palette
                set hidden3d
                set dgrid3d 50,50
                set datafile separator ','
                splot 'attempt_timings.csv' using 4:5:7 with l
EOF
}

copy

plotEndToEndTiming

plotAttemptTimings

plotShuffleBytesPerAttempt
mv *.svg ~/public_html/profiler_output
