#!/bin/bash
#format hadoop namenode
hadoop namenode -format

#start hadoop 
start-all.sh

#start hbase
start-hbase.sh

# Configure the working environment
cp /root/software/hbase-0.94.7/conf/hbase-site.xml /root/software/hadoop-1.1.2/conf/

export HADOOP_CLASSPATH=`/root/software/hbase-0.94.7/bin/hbase classpath`

#build jar and copy to hadoop lib
cd /home/summer/share/B649/HBaseInvertedIndexing/
ant clean
ant
cp dist/lib/cglHBaseMooc.jar /root/software/hadoop-1.1.2/lib/cglHBaseMooc.jar

# goto hadoop home 
cd /root/software/hadoop-1.1.2/

# Create all HBase Tables
hadoop jar lib/cglHBaseMooc.jar iu.ptihbaseapp.clueweb09.TableCreatorClueWeb09

#create one directory for mapreduce data input
rm /root/MoocHomeworks/HBaseWordCount/data/clueweb09/mrInput
mkdir -p /root/MoocHomeworks/HBaseWordCount/data/clueweb09/mrInput

#create input's metadata for HBase data loader
hadoop jar lib/cglHBaseMooc.jar iu.pti.hbaseapp.clueweb09.Helpers create-mr-input /root/MoocHomeworks/HBaseWordCount/data/clueweb09/files/ /root/MoocHomeworks/HBaseWordCount/data/clueweb09/mrInput/ 1

#copy metadata to Hadoop HDFS
hadoop dfs -copyFromLocal /root/MoocHomeworks/HBaseWordCount/data/clueweb09/mrInput/ /cw09LoadInput

# check
#hadoop dfs -ls /cw09LoadInput

#load data into HBase (takes 10-20 minutes to finish)
hadoop jar lib/cglHBaseMooc.jar iu.pti.hbaseapp.clueweb09.DataLoaderClueWeb09 /cw09LoadInput

#check 
#hadoop jar lib/cglHBaseMooc.jar iu.pti.hbaseapp.HBaseTableReader clueWeb09DataTable details string string string string 1 > dataTable1.txt

# run frequency index builder 
hadoop jar lib/cglHBaseMooc.jar iu.pti.hbaseapp.clueweb09.FreqIndexBuilderClueWeb09

#load page rank data set
hadoop jar lib/cglHBaseMooc.jar iu.pti.hbaseapp.clueweb09.PageRankTableLoader /root/MoocHomeworks/HBaseInvertedIndexing/resources/en0000-01and02.docToNodeIdx.txt  /root/MoocHomeworks/HBaseInvertedIndexing/resources/en0000-01and02_reset_idx_and_square_pagerank.out

#search for snapshot
hadoop jar lib/cglHBaseMooc.jar  iu.pti.hbaseapp.clueweb09.SearchEngineTester search-keyword snapshot



