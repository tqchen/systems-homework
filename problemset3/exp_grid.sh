#!/bin/bash

rm lcascipy.log
SN=800
for ncore in 100 200 400 600
do
    hadoop dfs -rmr data/result-$SN 
    echo "------------------------" >> lcascipy.log
    echo "start testing ncore=" $ncore "N=" $SN >>lcascipy.log
    spark-submit  --total-executor-cores 600 lcascipy.py data $SN data/result-$SN 1>>lcascipy.log
    echo "------------------------" >> lcascipy.log    
done

nscore=600
for SN in 100 200 400 800 1600
do
    hadoop dfs -rmr data/result-$SN	
    echo "------------------------" >> lcascipy.log
    echo "start testing ncore=" $ncore "N=" $SN >>lcascipy.log
    spark-submit  --total-executor-cores 600 lcascipy.py data $SN data/result-$SN 1>>lcascipy.log
    echo "------------------------" >> lcascipy.log
done


rm lcagrid.log

SN=800
for ncore in 100 200 400 600
do
    hadoop dfs -rmr data/gresult-$SN 
    echo "------------------------" >> lcagrid.log
    echo "start testing ncore=" $ncore "N=" $SN >>lcagrid.log
    spark-submit  --total-executor-cores 600 lcagrid.py data $SN 36 data/gresult-$SN 1>>lcagrid.log
    echo "------------------------" >> lcagrid.log    
done

nscore=600
for SN in 100 200 400 800 1600
do
    hadoop dfs -rmr data/gresult-$SN	
    echo "------------------------" >> lcagrid.log
    echo "start testing ncore=" $ncore "N=" $SN >>lcagrid.log
    spark-submit  --total-executor-cores 600 lcagrid.py data $SN 36 data/gresult-$SN 1>>lcagrid.log
    echo "------------------------" >> lcagrid.log
done


for SN in 1600 3200
do
    hadoop dfs -rmr data/result-$SN	
    echo "------------------------" >> lcalarge.log
    echo "start testing LCA scipy ncore=" $ncore "N=" $SN >>lcalarge.log
    spark-submit  --total-executor-cores 600 lcascipy.py data $SN data/result-$SN 1>>lcalarge.log
    echo "------------------------" >> lcalarge.log
done

for SN in 1600 3200
do
    hadoop dfs -rmr data/result-$SN	
    echo "------------------------" >> lcalarge.log
    echo "start testing LCA grid ncore=" $ncore "N=" $SN >>lcalarge.log
    spark-submit  --total-executor-cores 600 lcagrid.py data $SN 36 data/result-$SN 1>>lcalarge.log
    echo "------------------------" >> lcalarg.log
done



