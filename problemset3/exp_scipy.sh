#!/bin/bash
rm lcascipy.log
for ncore in 100 200 400 600
do
    for SN in 100 200 400 800 1600
    do
	hadoop dfs -rmr data/result-$SN	
	echo "------------------------" >> lcascipy.log
	echo "start testing ncore=" $ncore "N=" $SN >>lcascipy.log
	spark-submit  --total-executor-cores $ncore lcascipy.py data $ncore data/result-$SN 1>>lcascipy.log 2>>stderr.log
	echo "------------------------" >> lcascipy.log
    done
done
