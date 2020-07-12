# 2020-lastfm-popularity
Analysis of popularity according to scrobbles in lastfm using Apache Spark [Joaquín Larraín, Vicente Rojas, Roberto Tapia. Group 6]

# Overview

The main goal of the project is to analize data from lastfm to answer questions about popularity of artists and tags. Particularly, the questions that we studied and answered are:
* Which tags are the most popular?
* Given a tag, who are the most popular artists that have said tag?
* Which tags have a higher standard deviation in the popularity of their artists

# Data
Every row in the dataset contains information about a specific artist: id, country, tags, amount of listeners and number of scrobbles, this last one is of particular importance, as it is the amount of times that lastfm users have listened to that artist's track. For this reason, scrobbles were used in this project as a measure of an artist's popularity.

The dataset used in the project is available in csv format [here](https://www.kaggle.com/pieca111/music-artists-popularity), it has a size of approximately 196 MB and it consists of over 1.4 million artists.
# Methods
To answered the questions presented in the overview we used PySpark, mainly because it allowed us to easily implement the tools we needed, like filters, maps, reductions and aggregations.

We can divide the process to answer the question as follows:

## Setup and input

```python
spark = SparkSession.builder.appName("Proyecto").getOrCreate()
lines = spark.read.text("artists.csv").rdd.map(lambda r: r[0])
lines = lines.map(lambda linea: (linea.split(',')[2],linea.split(',')[6],linea.split(',')[7],linea.split(',')[8], linea.split(',')[9]))
lines = lines.filter(lambda linea: (linea[4]=='FALSE' and linea[0]!=''))
```
Here we create the spark session we'll use, and then proceed to load the csv file containing our dataset, after which we project only the columns of interest: an artist's id, their associated tags, number of listeners and scrobbles, and whether or not are ther multiple artists with the same name. Finally, we filter artists with a repeated name for simplicity in our calculations.

## Queries
```python
lines.cache()
popularTags(lines)
popularArtists(lines, tag)
tagsByDeviation(lines)
```
Now we cache our RDD, for it will be used multiple times, once for each of the function calls after the cache. Not caching this RDD increases the runtime in approximately 10 seconds.

### Popular Tags
```python
def popularTags(rddMap):
dataFrame = rddMap.toDF(['artist_lastfm', 'tags_lastfm','listeners_lastfm', 'scrobbles_lastfm', 'ambiguous_artist'])
dataFrame = dataFrame.withColumn('tags_lastfm', explode(split('tags_lastfm','; ')))
lines2 = dataFrame.rdd.map(list)
lines2 = lines2.map(lambda linea :(linea[1], int(linea[3])))
lines2 = lines2.reduceByKey(add)
lines2.coalesce(1).sortBy(lambda linea: linea[1], False).saveAsTextFile("Results-Popular Tags")
```
Here we create a dataframe from our RDD, we do this to use the explode method, which allows us to create a row for every tag an artist has associated. After that we go back to a RDD and we project only the name of the tags along with the amount of scrobbles. By doing this we have key-value pairs where the tag is the key and the number of scrobbles are the value. Thanks to the key-value pairs we can use reduceByKey to add the scrobbles for each particular tag. Finally we sort and output the result.

### Popular Artists by Tag
```python
lines = rddMap.map(lambda linea: (linea[0], linea[1].split('; '), int(linea[3])))
lines = lines.filter(lambda linea: Tag in linea[1])
lines.coalesce(1).sortBy(lambda linea: linea[2], False).saveAsTextFile("Results-Popular Artists")
```
We start by projecting only the artist name, a list with their tags, and their scrobbles, then we filter to keep only artists with the tag that we were looking for, and finally sort the result and save it as a text file.

### Tags by Deviation
```python
dataFrame = rddMap.toDF(['artist_lastfm', 'tags_lastfm','listeners_lastfm', 'scrobbles_lastfm', 'ambiguous_artist'])
dataFrame = dataFrame.withColumn('tags_lastfm', explode(split('tags_lastfm','; ')))
dataFrame = dataFrame.groupBy('tags_lastfm').agg(stddev_pop('scrobbles_lastfm'))
lines2 = dataFrame.rdd.map(list)
lines2.coalesce(1).sortBy(lambda linea: linea[1], False).saveAsTextFile("Results-Tags by Deviation")
```
Just like in popularTags, we start by generating a dataframe with multiple rows for each artists, but only one tag per row, after which we group by tags and use aggregation to find the scrobble standard deviation for each tag. Finally we transform the dataframe to an RDD, order it, and save it to a text file.




# Results

### Popular Tags
| Tag | Scrobbles |
|---|:---:|
| seen live| 75.708.482.958 |
| rock| 66.021.225.555 |
| alternative| 60.493.629.205 |
| indie| 49.710.840.958 |
| pop| 47.786.719.362 |
| electronic| 37.965.794.738 |
| american| 37.789.782.267 |
| alternative rock| 37.160.144.106 |
| 00s| 34.764.928.557 |
| male vocalists| 34.674.259.862 |

### Popular Artists by Tag (rock)

| Artist | Scrobbles |
|---|:---:|
| The Beatles | 517.126.254 |
| Radiohead | 499.548.797 |
| Coldplay | 360.111.850 |
| Muse | 344.838.631 |
| Arctic Monkeys | 332.306.552 |
| Pink Floyd | 313.236.119 |
| Linkin Park | 294.986.508 |
| Red Hot Chili Peppers | 293.784.041 |
| Lady Gaga |  285.469.647 |
| Metallica | 281.172.228 |

(Also outputs a list of each artist's tags, which has been omitted here)
### Tags by Deviation

|Tag | Standard deviation |
|---|:---:|
| linkin | 147.281.550,5 |
| cold play | 143.992.454,11572826 |
| Hollywood Pop | 108.578.359,5 |
| gayfish | 103.240.777,68459223 |
| Its Britney Bitch | 100.676.999,5 |
| umbrella | 93.925.358,88343942 |
| lp | 92.640.417,61620608 |
| slim shady | 92.537.331,97107212 |
| hayley williams | 87.853.126,0769334 |
| hollywood sadcore | 86.806.549,45046204 |



# Conclusion


