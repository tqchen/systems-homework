#!/bin/bash
rm lcagrid.log
for ncore in 100 200 400 600
do
    for SN in 100 200 400 800 1600
    do
	hadoop dfs -rmr data/result-$SN	
	echo "------------------------" >> lcagrid.log
	echo "start testing ncore=" $ncore "N=" $SN >>lcagrid.log
	spark-submit  --total-executor-cores $ncore lcagrid.py data $ncore data/result-$SN 36 1>>lcagrid.log 2>>stderr.log
	echo "------------------------" >> lcagrid.log
    done
done
