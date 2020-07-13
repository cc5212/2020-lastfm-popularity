# 2020-lastfm-popularity
Analysis of popularity according to scrobbles in [**last.fm**](https://www.last.fm) using Apache Spark [Joaquín Larraín, Vicente Rojas, Roberto Tapia. Group 6]

# Overview

The main goal of the project is to analize data from last.fm to answer questions about popularity of artists and tags. Particularly, the questions that we studied and answered are:
* Which tags are the most popular?
* Given a tag, who are the most popular artists that have said tag?
* Which tags have a higher standard deviation in the popularity of their artists

# Data
Every row in the dataset contains information about a specific artist: id, country, tags, amount of listeners and number of scrobbles. This last one is of particular importance, as it is the amount of times that last.fm users have listened to that artist's track. For this reason, scrobbles were used in this project as a measure of an artist's popularity. Scrobbles were picked over listeners due to scrobbles showing overall popularity or appeal of an artist, so if two tags have the same amount of listeners, but one has more scrobbles than the other, it shows that the second artist has a better appeal into their audience, who listen to their music more frequently. 

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

As seen from the results, the most popular tags are the ones associated with generic music genres like rock, alternative or indie. These tags usually have subgenres associated with them (ex: classic rock, alternative rock, etc). This makes sense because a wider variety of music and artists are part of these music genres. 

Looking over the results from the rock tag, they indeed show popular rock artists. Lady Gaga seems to be the one exception, however, despite being mainly associated with pop music, she also has some rock tracks, and because of her big audience, she also makes it into the list. This can show that for an artist to make it into the top of appearances into a specific tag they don’t necessarily need to be known for that tag, but rather have at least some tracks with that genre. 

Regarding the results seen in Tags by Deviation, most of the results are associated with a specific artist (linkin with Linkin Park, cold play with Coldplay, hayley williams with Hayley Williams, etc). This shows that these tags are most likely being used by both the original artists and by artists using their tag to try and get more views (by doing covers, or just using their tags), which make the standard deviation bigger due to the scrobble differences with the big artists. 

Overall, Spark helped us develop this project thanks to its easy management of DataFrames and the use of the aggregate and reduce methods, which made work more straightforward. Spark programming was also easy to implement, or at least easier for us than Uhadoop would have been. Thanks to the mentioned Spark tools, the implementation was not particularly hard, the challenge of this project was to pick the right framework (Spark) for the project and thinking over how to get the results we wanted.

As a way of further exploring this dataset, it would be possible to compare how this results would had been if the amount of listeners was used as the popularity metric for artists instead of scrobbles, or mixing up both metrics to get more specific results. 
