Job logs are saved as TFiles.  Pig script (formatting is sort of messed up when viewing in github) attached here tries to parse the TFiles directly instead of making a text copy and parsing the logs.

It internally uses TFileStorage (UDF attached) to parse the TFiles.


All we need to provide is the application log location (TFile location).


GNUPLOT scripts are not included here.  Probably, we can use the same things which are in previous directory. Plot.sh is attached here, which might not be a good script (but just for reference)
Sample SVG is attached (https://github.com/rajeshbalamohan/shuffle_parser/blob/master/tfile_parsing/plotAttemptTimings.svg).
