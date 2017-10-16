# PageRank Wikipedia

The script finds top 100 pages in Wikipedia along with the universities

## Getting Started

Install Spark on your system or on your Hadoop cluster. Spark with HDFS is recommended.

### Prerequisites

Install Java and set JAVA_HOME.


## How to run

For finding the list of universities

spark-submit  --master spark://ip-172-31-20-18.ec2.internal:7077 --driver-memory <driver-memory> pagerank.py --inp <.tsv file containging xml> --link <directory to store intemediate links file> --iter 2 –output <results output file> --mem <RAM> --proc < max CPU cores> --query University

Top 100 pages

./spark-2.2.0-bin-hadoop2.7/bin/spark-submit  --master spark://ip-172-31-20-18.ec2.internal:7077 --driver-memory <driver-memory> pagerank.py --inp <.tsv file containging xml> --link <directory to store intemediate links file> --iter 2 –output <results output file> --mem <RAM> --proc < max CPU cores>
