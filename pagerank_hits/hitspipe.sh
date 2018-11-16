#!/usr/bin/env bash
hdfs dfs -rm -r hw_pagerank/HITS
hdfs dfs -mkdir hw_pagerank/HITS
echo "HITS loop"
hadoop jar pagerank_hits.jar HITSJob hw_pagerank/ParsedData hw_pagerank/HITS 4
echo "Calculating top by authority"
hadoop jar pagerank_hits.jar SortJobIn hw_pagerank/HITS/fin/part* hw_pagerank/HITS/resultA
echo "Calculating top by hubness"
hadoop jar pagerank_hits.jar SortJobOut hw_pagerank/HITS/fin/part* hw_pagerank/HITS/resultH
echo "Dumping"
hdfs dfs -cat hw_pagerank/HITS/resultA/part* | head -n 30 > authorityTop.txt
hdfs dfs -cat hw_pagerank/HITS/resultH/part* | head -n 30 > hubnessTop.txt
