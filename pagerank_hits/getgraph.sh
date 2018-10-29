#!/usr/bin/env bash

hdfs dfs -rm -r hw_pagerank
hdfs dfs -mkdir hw_pagerank
echo "Generating graph"
hadoop jar pagerank_hits.jar GetLinksJob /data/infopoisk/hits_pagerank/docs-*.txt hw_pagerank/ParsedData
echo "Counting unique links"
hadoop jar pagerank_hits.jar UniqueLinkCountJob hw_pagerank/ParsedData/unique-* hw_pagerank/unique
echo "Done"