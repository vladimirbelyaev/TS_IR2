#!/usr/bin/env bash
hdfs dfs -rm -r hw_pagerank/PageRank
hdfs dfs -mkdir hw_pagerank/PageRank
echo "Initialising PageRank iter_0"
hadoop jar pagerank_hits.jar InitPageRankJob hw_pagerank/ParsedData/part-* hw_pagerank/PageRank/iter_0
echo "Executing PageRank loop"
hadoop jar pagerank_hits.jar PageRankJob hw_pagerank/PageRank/iter_ 7
echo "Sorting"
hadoop jar pagerank_hits.jar SortJobOut hw_pagerank/PageRank/iter_fin hw_pagerank/PageRank/result
echo "Extracting results"
hdfs dfs -cat hw_pagerank/PageRank/result/part* | head -n 30 >> pagerank_result.txt
echo "Testing"
hadoop jar pagerank_hits.jar TestingPageRankJob hw_pagerank/PageRank/iter_fin/part* hw_pagerank/PageRank/fin_test
hdfs dfs -cat hw_pagerank/PageRank/fin_test/part*
hdfs dfs -cat hw_pagerank/PageRank/iter_6/leak*