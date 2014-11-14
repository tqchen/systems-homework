from pyspark import SparkContext
import networkx as nx
import csv
import sys

def load_csv(filename):
    """Helper function to load CSV data from a GZipped file"""
    csvfile = csv.reader(open(filename))
    header_line = csvfile.next()
    print('Reading from CSV file %s with headers %s' % (filename, header_line));
    #print 'Reading from CSV file {fn} with headers {h}'.format(fn=filename, h=header_line)
    return csvfile

def load_graph(dpath):
    g = nx.DiGraph()    
    for pid, year in load_csv(dpath+'/papers.csv'):
        g.add_node(int(pid), year=int(year))    

    for p1, p2 in load_csv(dpath+'/cites.csv'):
        try:
            src = int(p1)
            dst = int(p2)
            if g.node[src]['year'] >= g.node[dst]['year'] and src != dst:
                g.add_edge(src, dst)
        except KeyError:
            pass
    return g

def map_dist(dist, k):
    return [(k,t,d) for t,d in dist.iteritems()]

if len(sys.argv) < 2:
    print 'Usage:<path>'
    exit(-1)

spark = SparkContext("local", "SparkLCA")
g = load_graph(sys.argv[1])
N = 50

print 'finish loading graph data'
seeds = spark.parallelize([p for p in g.nodes() if p <= N])
distg = spark.broadcast(g)
print 'start working'
cite_depth = seeds.flatMap(lambda k: map_dist(nx.single_source_shorest_path(distg.value, k)))

print 'finish'

