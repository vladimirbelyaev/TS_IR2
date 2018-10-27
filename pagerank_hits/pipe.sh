#!/usr/bin/env bash
hdfs dfs -put -f pagerank_hits.jar
hdfs dfs -rm -r hw_pagerank

hadoop jar pagerank_hits.jar GetLinksJob /data/infopoisk/hits_pagerank/docs-000.txt hw_pagerank
hadoop jar pagerank_hits.jar WordCountJob hw_pagerank/unique-* hw_pagerank/unique
hadoop jar pagerank_hits.jar InitPageRankJob hw_pagerank/part-* hw_pagerank/iter_0