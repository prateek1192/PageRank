import re
import itertools
import sys
from operator import add
from pyspark import SparkContext
from pyspark import SparkConf
import argparse


def linksShare(urls, rank):
    """Share of other links in this link"""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """If an link exists, it is taken into memory as a tuple"""
    if urls:
        parts = re.split(r'\t', urls)
        return parts[0], parts[1]

def processRecord(record):
    """Parses an ajacency list to give the list of links"""
    pageName = record.split("\t")[1]
    links = re.findall(r'<target>(.*?)\</target>', record)
    str = ""
    for link in links:
        if link and not link == pageName and not link.startswith('File:') and not ':' in link:
            str_to_add = pageName+"\t"+link
            if link != links[-1]:
                str_to_add = str_to_add+"\n"
            else:
                str_to_add = str_to_add
            str+=str_to_add
    return str


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Page Rank Calculator',
                                     add_help='How to use',
                                     prog='./bin/pyspark pagerank.py --inp XML file --link path to store the XML file --iter number of iterations --output final output file --mem Memory')

    # Required params
    parser.add_argument("--inp", type=str, required=True, help="XML File")
    parser.add_argument("--link", type=str, required=True, help="Path to store the links file")
    parser.add_argument("--iter", type=int, required=True, help="Number of iterations.")
    parser.add_argument("--output", type=str, required=True, help="Final output file.")


    # Optional params
    parser.add_argument("--query", type=str, required=False, help="To list results containing a particular substring.")
    parser.add_argument("--mem", type=int, default=1, help="Spark Executor Memory in GB")
    parser.add_argument("--proc", type=int, default=4, help="CPU cores default 4")

    args = vars(parser.parse_args())

    conf = SparkConf()
    conf.setAppName("PageRank")
    conf.set("spark.executor.memory", str(args['mem']) + "g")
    conf.set("spark.cores.max", )
    # Initialize the spark context.
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Input file containing data from wikipedia
    file = args['inp']
    lines = sc.textFile(file, 1)

    # Store the output parsed from XML file which would be in the form of URL neighbour URL
    neighborFile = args['link']

    # Create a text file of the form URL Link
    # The empty pages are not handled separately and would be ouput as a blank line
    fileCreator = lines.map(lambda line: processRecord(line)).saveAsTextFile(neighborFile)

    # Loads all URLs from input file and initialize their neighbors.
    file2 = neighborFile+"/*"
    lines = sc.textFile(file2, 1)
    links = lines.map((lambda urls:parseNeighbors(urls) if parseNeighbors(urls) is not None else None)).filter(lambda x: x is not None).distinct().groupByKey().cache()

    # Assigns page rank of 1 to all vertexes having links
    ranks = links.map(lambda to_vertex: (to_vertex[0], 1.0))

    # Computes page rank as specified nmber of times.
    for iteration in range(int(args['iter'])):
        # Calculates URL contributions to the rank of other URLs.
        share = links.join(ranks).flatMap(
            lambda link_links_share: linksShare(link_links_share[1][0], link_links_share[1][1]))

        # New Rank based on neighbors.
        ranks = share.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)


    # Sort the result

    output = ranks.sortBy(lambda x: x[1]).zipWithIndex().   collect()
    results=""
    # When we need to parse a certain subset like university
    if args['query']:
        i = 1
        for (link, rank) in output:
            if i > 100:
                break
            if args['query'] == (link[0].encode('utf-8')) :
                rank = len(output) - rank
                results+="%s has rank: %s.\n" % (link, rank)
                rank= int(i)
                results+="%s has query rank: %s.\n" % (link, rank)
                i=i+1
    # For simple list of top 100
    else:
        for (link, rank) in output[-100:]:
            rank = len(output) - rank
            results+="%s has rank: %s.\n" % (link, rank)

    f = open(args['output'], 'w')
    f.write(results)
    f.close()

    sc.stop()

