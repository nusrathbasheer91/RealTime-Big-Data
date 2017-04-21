tweets = LOAD 'hw4/input.txt' AS (tweet:chararray);
tweetsl= foreach tweets generate flatten(LOWER(tweet)) as tweet;
mats = foreach tweetsl generate (tweet matches '.*hackathon.*' ? 1:0 ) as hack, (tweet matches '.*chicago.*' ? 1:0) as chic, (tweet matches '.*java.*' ? 1:0 ) as jav, (tweet matches '.*dec.*' ?1 :0) as dec;
cnts = group mats all;
fnl = FOREACH cnts GENERATE FLATTEN(TOBAG(CONCAT('Hackathon',' ',(chararray)SUM(mats.hack)),CONCAT('Java',' ',(chararray)SUM(mats.jav)),CONCAT('Chicago',' ',(chararray)SUM(mats.chic)),CONCAT('Dec',' ',(chararray)SUM(mats.dec))));
dump fnl;
