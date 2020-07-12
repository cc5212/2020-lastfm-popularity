import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,explode,stddev_pop
from operator import add

"""
Formato:
mbid[0],   artist_mb[1], artist_lastfm[2],   country_mb[3], country_lastfm[4], tags_mb[5], tags_lastfm[6],   listeners_lastfm[7],   scrobbles_lastfm[8], ambiguous_artist[9].

Solo me interesan: artist_lastfm, tags_lastfm, listeners_lastfm, scrobbles_lastfm, y amniguous_artist
2,6,7,8,9
"""
#Most Popular Tags
#We define a tag's popularity as the sum of scrobbles for all artists with the given tag
def popularTags(rddMap):
    dataFrame = rddMap.toDF(['artist_lastfm', 'tags_lastfm','listeners_lastfm', 'scrobbles_lastfm', 'ambiguous_artist'])
    dataFrame = dataFrame.withColumn('tags_lastfm', explode(split('tags_lastfm','; ')))
    #dataFrame.show()
    lines2 = dataFrame.rdd.map(list)
    lines2 = lines2.map(lambda linea :(linea[1], int(linea[3])))
    lines2 = lines2.reduceByKey(add)
    lines2.coalesce(1).sortBy(lambda linea: linea[1], False).saveAsTextFile("Results-Popular Tags")

#Most Popular Artists in a Tag
def popularArtists(rddMap, Tag):
    lines = rddMap.map(lambda linea: (linea[0], linea[1].split('; '), int(linea[3])))
    lines = lines.filter(lambda linea: Tag in linea[1])
    lines.coalesce(1).sortBy(lambda linea: linea[2], False).saveAsTextFile("Results-Popular Artists")

#Tags ordered by standard deviation
def tagsByDeviation(rddMap):
    dataFrame = rddMap.toDF(['artist_lastfm', 'tags_lastfm','listeners_lastfm', 'scrobbles_lastfm', 'ambiguous_artist'])
    dataFrame = dataFrame.withColumn('tags_lastfm', explode(split('tags_lastfm','; ')))
    dataFrame = dataFrame.groupBy('tags_lastfm').agg(stddev_pop('scrobbles_lastfm'))
    lines2 = dataFrame.rdd.map(list)
    lines2.coalesce(1).sortBy(lambda linea: linea[1], False).saveAsTextFile("Results-Tags by Deviation")

if __name__ == "__main__":   
    if len(sys.argv) != 2:
        print('Usage: proyecto.py <Tag>')
    tag = sys.argv[1] 
    start = time.time()
    spark = SparkSession.builder.appName("Proyecto").getOrCreate()

    lines = spark.read.text("artists.csv").rdd.map(lambda r: r[0])
    lines = lines.map(lambda linea: (linea.split(',')[2],linea.split(',')[6],linea.split(',')[7],linea.split(',')[8], linea.split(',')[9]))
    #FORMATO: artist_lastfm[0], tags_lastfm[1],listeners_lastfm[2], scrobbles_lastfm[3], ambiguous_artist[4] 
    lines = lines.filter(lambda linea: (linea[4]=='FALSE' and linea[0]!=''))
    lines.cache()
    popularTags(lines)
    popularArtists(lines, tag)
    tagsByDeviation(lines)
    end = time.time()
    print(end-start)
    
   