Team
===

Tianqi Chen, 1323348, tqchen@cs.washington.edu
Tianyi Zhou, 1323375, tianzh@cs.washington.edu

Summary
===
This is an pyspark implementation of LCA problem, there are two implementations

* lcagrid.py  distribute task in a matrix grid and calculate the LCA between pairs in each grid
* lcascipy.py calculate shortest path distributed, then use two mapreduce step to get the result

Usage
===
* Put paper.csv and cites.csv under same folder, say data
* spark-submit lcascipy.py path-to-data-folder N [out-hdfs-path]
  - If out-hdfs-path is not specified, the result will be stored into data-folder/result-N.csv
  - For large task, it is recommended to store result in hdfs
* The first time running, a npygraph.pkl file will be created in the data folder, latter runs will directly load from pickle file and will be much faster
* Example usage assume data are in folder data
  - ``spark-submit lcascipy.py data 1000`` will put result in data/result-1000.csv
  - ``spark-submit lcascipy.py data 1000 result-1000`` will put result in HDFS result-1000/part-*
