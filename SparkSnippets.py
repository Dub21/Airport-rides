#import
from __future__ import print_function, division
import pyspark
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, expr, when, abs, avg, sqrt, cos, radians, pow, sin, lit 
import re, sys, math 
from matplotlib import pyplot as plt
import numpy as np
import matplotlib.pyplot as mplt
import matplotlib.ticker as mtick


#import of data

rdd = sc.textFile("Downloads/2010_03.trips.csv")
rdd = rdd.map(lambda s: s.split(" "))
rdd = rdd.map(lambda rec: (int(rec[0]), float(rec[1]), float(rec[2]), float(rec[2]), float(rec[3]),float(rec[4]), float(rec[5]), float(rec[6])))
lines = sc.textFile("Downloads/2010_03.trips_pure.csv")



#How many unique taxis are present in the data?

rdd.groupBy(lambda rec: (rec[0])).count()



#How many trips did taxi 46 complete?

rdd.filter(lambda rec: (rec[0] == 46)).count()



#Which taxi participated in the most trips?

sorted(rdd.filter(lambda rec: (rec[0])).countByKey().items())



#What is the distance of the longest trip?

def distance(line):
    fields = line.split(" ")  # split line into list at comma positions
    ID = int(fields[0])  # extract and typecast relevant fields
    dlon = math.radians(float(fields[6]) - (float(fields[3])))
    dlat = math.radians(float(fields[5]) - (float(fields[2])))
    earth_radius = 6373.0
    a = (math.sin(dlat/2)** 2) + math.cos(math.radians((float(fields[5]))))\
    * math.cos(math.radians((float(fields[2])))) * (math.sin(dlon/2)**2)
    Distance = float(earth_radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))
    return(ID, Distance)

rddLongestDistance = lines.map(distance)
rddLongestDistance.sortBy(lambda x: x[1], ascending=False).take(1)



#Which three taxis completed the longest trips on average?

rddTotalDistanceByTaxi = rddLongestDistance.mapValues(lambda x: (x,1))\
                        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
rddAveragesDistanceByTaxi = rddTotalDistanceByTaxi.mapValues(lambda x: x[0] / x[1])
rddAveragesDistanceByTaxi.sortBy(lambda x: x[1], ascending=False).take(3)



#On what date did the most trips start?

def date(line):
    fields = line.split(" ")  # split line into list at comma positions
    dates = float(fields[1])  # extract and typecast relevant fields
    numFriends = 1
    return(dates, numFriends)

rddMosteDate = lines.map(date)

totalsByDates = rddMosteDate \
    .mapValues(lambda x: (x,1)) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) 
totalsByDates.sortBy(lambda x: x[1], ascending=False).take(2)



#Frequency distribution

def plotHistogramData(data):
    binSides, binCounts = data

    N = len(binCounts)
    ind = np.arange(N)
    width = 1

    fig, ax = mplt.subplots()
    rects1 = ax.bar(ind+0.5, binCounts, width, color='b')

    ax.set_ylabel('Frequencies')
    ax.set_title('Histogram')
    ax.set_xticks(np.arange(N+1))
    ax.set_xticklabels(binSides)
    ax.xaxis.set_major_formatter(mtick.FormatStrFormatter('%.2e'))
    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2e'))

    mplt.show()
    
def create_hist(rdd_histogram_data):
  """Given an RDD.histogram, plot a pyplot histogram"""
  heights = np.array(rdd_histogram_data[1])
  full_bins = rdd_histogram_data[0]
  mid_point_bins = full_bins[:-1]
  widths = [abs(lit(i - j)) for i, j in zip(full_bins[:-1], full_bins[1:])]
  bar = plt.bar(mid_point_bins, heights, width=widths, color='b')
  return bar

rddDistribution=rddLongestDistance.map(lambda rec: (rec[1])).filter(lambda x: x<100)

plotHistogramData(rddDistribution.histogram(100))

