# Databricks notebook source exported at Tue, 19 Jul 2016 23:06:33 UTC
# MAGIC %md 
# MAGIC ![titre](http://networkdata.ics.uci.edu/images/title.png)
# MAGIC 
# MAGIC #Creating GraphFrames - Neural Network of the nematode C. Elegans
# MAGIC 
# MAGIC This page is used to explore graph data API (GraphFrames) in python using the celegansneural dataset of "UCI Network Data Repository". 
# MAGIC The file celegansneural.gml describes a weighted, directed network representing the neural network of C. Elegans.  The data were taken from the web site of Prof. Duncan Watts at [Columbia University](http://cdg.columbia.edu/cdg/datasets).  The nodes in the original data were not consecutively numbered, so they have been renumbered to be consecutive. The original node numbers from Watts' data file are retained as the labels
# MAGIC of the nodes.  Edge weights are the weights given by Watts.
# MAGIC 
# MAGIC >  J. G. White, E. Southgate, J. N. Thompson, and S. Brenner, "The structure of the nervous system of the nematode C. Elegans", Phil. Trans. R. Soc. London 314, 1-340 (1986).
# MAGIC 
# MAGIC >  D. J. Watts and S. H. Strogatz, "Collective dynamics of small-world'networks", Nature 393, 440-442 (1998).

# COMMAND ----------

# MAGIC %python
# MAGIC # Downloads data at provided url
# MAGIC import urllib2
# MAGIC url = 'http://networkdata.ics.uci.edu/data/celegansneural/celegansneural.gml'
# MAGIC response = urllib2.urlopen(url)
# MAGIC print response.info()
# MAGIC html = response.read()
# MAGIC response.close()

# COMMAND ----------

# Splits html lines by ] then by "directed 1" keeping only second part (to lose .gml headers) and filter out empty arrays
dataRDD = sc.parallelize(html.split("]")).map(lambda x: x.split("directed 1")[-1]).filter(lambda x: x != "").filter(lambda x: x != "\n")
print "First 3 elements in list created are: "
for i in dataRDD.take(3):
  print i
print "This RDD has {0} elements.".format(dataRDD.count())

# COMMAND ----------

# Here we separate out nodes and vertices into different datasets
nodesRDD = dataRDD.filter(lambda x: "node" in x)
verticesRDD = dataRDD.filter(lambda x: "edge" in x)
print "The nodesRDD has {0} elements, while the verticesRDD has {1} elements.".format(nodesRDD.count(), verticesRDD.count())

# COMMAND ----------

import re

def doIt1(x):
  x = x.replace('\n','').replace('node','').replace('id ', '{"id": ').replace('label ', ', "label": ').replace('[', '').strip()
  x = re.sub("\s\s+", " ", x) + '}'
  y = '"' + re.findall(r'\b\d+\b', x)[0] + '"'
  x = re.sub('\d+', y, x, count=1)
  x = x.replace('""','"')
  return x

nodesRDD =  nodesRDD.map(doIt1)
nodesRDD.take(10)
nodes = nodesRDD.collect()

# COMMAND ----------

# MAGIC %python
# MAGIC f = open('nodes.txt', 'w')
# MAGIC print >> f, '{\n', '"nodes": {0} '.format(nodes), '],'
# MAGIC f.close()

# COMMAND ----------

def doIt2(x):
  
  x = '{' + re.sub("\s\s+", " ", x) + '}'
  x = x.replace('\n','').replace('edge','').replace('[', '').strip().replace('source ', '"source": "').replace(' target ', '", "target": "').replace(' value ', '", "value":')
  x = re.sub("\s\s+", " ", x)
  return x
verticesRDD = verticesRDD.map(doIt2)
verticesRDD.take(10)
vertices = verticesRDD.collect()

# COMMAND ----------

# MAGIC %python
# MAGIC f = open('vertices.txt', 'w')
# MAGIC print >> f, '"links": {0} '.format(vertices), '}'
# MAGIC f.close()
