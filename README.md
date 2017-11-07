# ACM SIGSPATIAL Cup 2016
## Problem Definition 

 - Input: A collection of New York City Yellow Cab taxi trip records spanning January 2009 to June 2015. The source data may be clipped to an envelope encompassing the five New York City boroughs in order to remove some of the noisy error data (e.g., latitude 40.5N – 40.9N, longitude 73.7W – 74.25W).

 - Output: A list of the fifty most significant hot spot cells in time and space as identified using the Getis-Ord  statistic.
[![N|Solid](http://sigspatial2016.sigspatial.org/giscup2016/gfx/image004.png)](https://nodesource.com/products/nsolid)


## System Requirement:
* scala 2.11.8
* Apache Spark pre-built with hadoop spark-2.2.0-bin-hadoop2.7

## Guidelines
* Please use local file system location as input and output path.
* If you are using cluster, make sure the input file location is consistent across nodes.
* Set master URL if you running in cluster, so that it would distribute computation across nodes
* The input csv file assumed to contain a header, so please use input csv file with header. Please download the input files from https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-01.csv

