# Netflix Recommendation System

![Netflix logo - Matko Soric](https://raw.githubusercontent.com/matkosoric/Netflix-Recommendation-System/master/src/main/resources/images/netflix-logo.png?raw=true "Netflix logo - Matko Soric")


Netflix prize was an open competition for the best collaborative filtering algorithm, started in 2006.
It was won by BellKor's Pragmatic Chaos team from AT&T Labs in 2009.
This Spark application will user Spark's built-in ALS algorithm to create a recommendation model for the data set from competition.

Preprocessing1.scala will create four column dataframe (movieId, userId, rating, date) from the original data and write it to the intermediate folder.
Preprocessing2.scala will use data from intermediate step and write it in a snappy compressed parquet format.


[Netflix prize](https://en.wikipedia.org/wiki/Netflix_Prize)

### Dataset

Netflix does not provides access to the original dataset, probably due to the legal issues. Nonetheless, it can be downloaded from the archived UCI ML repository:
[Netflix Prize Data Set](https://web.archive.org/web/20090925184737/http://archive.ics.uci.edu/ml/datasets/Netflix+Prize)


### Tools

[Spark 2.4.0 MLlib](https://spark.apache.org/docs/latest/ml-guide.html)



### Results

![Training process - Matko Soric](https://raw.githubusercontent.com/matkosoric/Netflix-Recommendation-System/master/src/main/resources/images/training-screenshoot.png?raw=true "Training process - Matko Soric")



<pre><code>
+-------+-------+------+----------+----+-----------------+
|movieId|userId |rating|date      |year|title            |
+-------+-------+------+----------+----+-----------------+
|5317   |2354291|3     |2005-07-05|2000|Miss Congeniality|
|5317   |185150 |2     |2005-07-05|2000|Miss Congeniality|
|5317   |868399 |3     |2005-07-05|2000|Miss Congeniality|
|5317   |1026389|5     |2005-07-06|2000|Miss Congeniality|
|5317   |2173336|4     |2002-05-18|2000|Miss Congeniality|
|5317   |364518 |2     |2002-05-02|2000|Miss Congeniality|
|5317   |1400980|1     |2005-07-07|2000|Miss Congeniality|
|5317   |1392773|4     |2003-08-18|2000|Miss Congeniality|
|5317   |2473170|4     |2005-07-13|2000|Miss Congeniality|
|5317   |1625755|4     |2005-07-07|2000|Miss Congeniality|
|5317   |1744873|3     |2005-07-07|2000|Miss Congeniality|
|5317   |53679  |4     |2005-07-08|2000|Miss Congeniality|
|5317   |712664 |3     |2001-08-03|2000|Miss Congeniality|
|5317   |1990901|4     |2001-07-08|2000|Miss Congeniality|
|5317   |2308631|4     |2005-07-08|2000|Miss Congeniality|
|5317   |479062 |5     |2005-07-08|2000|Miss Congeniality|
|5317   |949827 |3     |2005-07-08|2000|Miss Congeniality|
|5317   |1907667|4     |2001-08-03|2000|Miss Congeniality|
|5317   |554875 |3     |2003-10-02|2000|Miss Congeniality|
|5317   |1437310|3     |2005-08-01|2000|Miss Congeniality|
|5317   |1490015|4     |2005-07-09|2000|Miss Congeniality|
|5317   |697185 |5     |2005-07-09|2000|Miss Congeniality|
|5317   |2119485|5     |2005-07-10|2000|Miss Congeniality|
|5317   |1500738|3     |2002-07-27|2000|Miss Congeniality|
|5317   |618502 |4     |2005-07-10|2000|Miss Congeniality|
|5317   |69867  |3     |2001-07-21|2000|Miss Congeniality|
|5317   |1402412|3     |2001-07-21|2000|Miss Congeniality|
|5317   |1365892|2     |2005-07-11|2000|Miss Congeniality|
|5317   |1601783|3     |2001-10-15|2000|Miss Congeniality|
|5317   |1789535|3     |2005-07-11|2000|Miss Congeniality|
|5317   |2258892|3     |2005-07-11|2000|Miss Congeniality|
|5317   |1993470|3     |2005-07-11|2000|Miss Congeniality|
|5317   |706360 |1     |2005-07-11|2000|Miss Congeniality|
|5317   |306466 |3     |2001-05-19|2000|Miss Congeniality|
|5317   |2384705|4     |2005-07-12|2000|Miss Congeniality|
|5317   |1481271|2     |2001-08-04|2000|Miss Congeniality|
|5317   |1142821|2     |2004-07-01|2000|Miss Congeniality|
|5317   |506696 |2     |2005-07-12|2000|Miss Congeniality|
|5317   |2385622|4     |2005-07-12|2000|Miss Congeniality|
|5317   |1987434|3     |2005-07-12|2000|Miss Congeniality|
+-------+-------+------+----------+----+-----------------+
only showing top 40 rows

We have 100480507 reviews, performed by 480189 users, on a collection of 17770 movies 


Top 50 movies by minimum, maxminum, and average score, and number of reviews
+---------------------------------------------------------------------------+--------+--------+------------+----------+
|title                                                                      |minScore|maxScore|averageScore|numReviews|
+---------------------------------------------------------------------------+--------+--------+------------+----------+
|Lord of the Rings: The Return of the King: Extended Edition                |1       |5       |4.723       |73335     |
|The Lord of the Rings: The Fellowship of the Ring: Extended Edition        |1       |5       |4.717       |73422     |
|Lord of the Rings: The Two Towers: Extended Edition                        |1       |5       |4.703       |74912     |
|Lost: Season 1                                                             |1       |5       |4.671       |7249      |
|Battlestar Galactica: Season 1                                             |1       |5       |4.639       |1747      |
|Fullmetal Alchemist                                                        |1       |5       |4.605       |1633      |
|Trailer Park Boys: Season 4                                                |1       |5       |4.6         |25        |
|Trailer Park Boys: Season 3                                                |1       |5       |4.6         |75        |
|Tenchi Muyo! Ryo Ohki                                                      |1       |5       |4.596       |89        |
|The Shawshank Redemption: Special Edition                                  |1       |5       |4.593       |139660    |
|Veronica Mars: Season 1                                                    |1       |5       |4.592       |1238      |
|Ghost in the Shell: Stand Alone Complex: 2nd Gig                           |1       |5       |4.586       |220       |
|Arrested Development: Season 2                                             |1       |5       |4.582       |6621      |
|The Simpsons: Season 6                                                     |1       |5       |4.581       |8426      |
|Inu-Yasha                                                                  |1       |5       |4.554       |1883      |
|Lord of the Rings: The Return of the King: Extended Edition: Bonus Material|2       |5       |4.552       |125       |
|Lord of the Rings: The Return of the King                                  |1       |5       |4.545       |134284    |
|Star Wars: Episode V: The Empire Strikes Back                              |1       |5       |4.544       |92470     |
|The Simpsons: Season 5                                                     |1       |5       |4.543       |17292     |
|Fruits Basket                                                              |1       |5       |4.539       |681       |
|The Sopranos: Season 5                                                     |1       |5       |4.534       |21043     |
|Family Guy: Vol. 2: Season 3                                               |1       |5       |4.528       |21385     |
|The Simpsons: Season 4                                                     |1       |5       |4.523       |18413     |
|Ah! My Goddess                                                             |3       |5       |4.522       |23        |
|Family Guy: Freakin' Sweet Collection                                      |1       |5       |4.516       |10027     |
|Band of Brothers                                                           |1       |5       |4.513       |37514     |
|Gladiator: Extended Edition                                                |1       |5       |4.513       |9599      |
|Raiders of the Lost Ark                                                    |1       |5       |4.504       |118212    |
|Star Wars: Episode IV: A New Hope                                          |1       |5       |4.504       |85184     |
|Seinfeld: Season 3                                                         |1       |5       |4.502       |9258      |
|CSI: Season 4                                                              |1       |5       |4.497       |11933     |
|Dead Like Me: Season 2                                                     |1       |5       |4.487       |3985      |
|Gilmore Girls: Season 4                                                    |1       |5       |4.484       |2670      |
|Family Guy: Vol. 1: Seasons 1-2                                            |1       |5       |4.484       |23254     |
|24: Season 2                                                               |1       |5       |4.482       |23767     |
|The West Wing: Season 3                                                    |1       |5       |4.474       |6633      |
|CSI: Season 3                                                              |1       |5       |4.472       |12921     |
|Six Feet Under: Season 4                                                   |1       |5       |4.47        |11829     |
|Samurai Champloo                                                           |1       |5       |4.468       |2000      |
|The Sopranos: Season 4                                                     |1       |5       |4.467       |35062     |
|24: Season 3                                                               |1       |5       |4.467       |17783     |
|Inu-Yasha: The Movie 3: Swords of an Honorable Ruler                       |1       |5       |4.465       |796       |
|Stargate SG-1: Season 8                                                    |1       |5       |4.462       |1812      |
|Anne of Green Gables: The Sequel                                           |1       |5       |4.462       |701       |
|Star Wars: Episode VI: Return of the Jedi                                  |1       |5       |4.461       |88846     |
|Lord of the Rings: The Two Towers                                          |1       |5       |4.461       |151245    |
|The Sopranos: Season 2                                                     |1       |5       |4.46        |40581     |
|Schindler's List                                                           |1       |5       |4.458       |101141    |
|South Park: Season 6                                                       |1       |5       |4.456       |3699      |
|The West Wing: Season 4                                                    |1       |5       |4.453       |5122      |
+---------------------------------------------------------------------------+--------+--------+------------+----------+
only showing top 50 rows

+-------+-----------------+------------------+------------------+------------------+--------------------+
|summary|          movieId|            userId|            rating|              year|               title|
+-------+-----------------+------------------+------------------+------------------+--------------------+
|  count|         20097121|          20097121|          20097121|          20097121|            20097121|
|   mean|9070.073551132025|1322675.5914950704| 3.604178578613325|1993.9113338174159|            Infinity|
| stddev|5132.322521683057| 764438.8469292417|1.0852571208494843| 12.40231428357574|                 NaN|
|    min|                1|                 6|                 1|              1896|'Allo 'Allo!: Ser...|
|    max|            17770|           2649429|                 5|              2005|                 sex|
+-------+-----------------+------------------+------------------+------------------+--------------------+

+-------+-----------------+------------------+------------------+------------------+--------------------+
|summary|          movieId|            userId|            rating|              year|               title|
+-------+-----------------+------------------+------------------+------------------+--------------------+
|  count|         80382421|          80382421|          80382421|          80382421|            80382421|
|   mean|9071.107159785595|1322442.1072478024| 3.604327891542356|1993.9115754177149|            Infinity|
| stddev|5131.776309358263| 764561.1571568286|1.0852045888136235|12.400461644527427|                 NaN|
|    min|                1|                 6|                 1|              1896|'Allo 'Allo!: Ser...|
|    max|            17770|           2649429|                 5|              2005|                 sex|
+-------+-----------------+------------------+------------------+------------------+--------------------+

+-------+-------+------+----------+----+--------------+-----------+
|movieId|userId |rating|date      |year|title         |prediction |
+-------+-------+------+----------+----+--------------+-----------+
|148    |10817  |4     |2005-09-06|2001|Sweet November|0.30987525 |
|148    |227152 |1     |2004-06-17|2001|Sweet November|0.31229064 |
|148    |426301 |5     |2005-03-26|2001|Sweet November|0.8262879  |
|148    |674543 |4     |2004-01-22|2001|Sweet November|0.52029485 |
|148    |794227 |5     |2005-04-17|2001|Sweet November|0.45645532 |
|148    |929895 |2     |2005-03-09|2001|Sweet November|0.20645303 |
|148    |946380 |3     |2005-08-29|2001|Sweet November|0.6212619  |
|148    |1025592|1     |2004-07-13|2001|Sweet November|0.57625777 |
|148    |1326936|3     |2004-08-06|2001|Sweet November|0.088862196|
|148    |1353371|3     |2003-11-04|2001|Sweet November|0.70023006 |
|148    |1392773|3     |2005-09-12|2001|Sweet November|0.36370406 |
|148    |1698928|3     |2004-03-24|2001|Sweet November|0.07004277 |
|148    |1718487|2     |2005-12-04|2001|Sweet November|0.25365177 |
|148    |1907843|5     |2004-05-15|2001|Sweet November|0.4917534  |
|148    |1931453|5     |2003-12-23|2001|Sweet November|0.81461483 |
|148    |2088272|3     |2004-06-02|2001|Sweet November|0.62885904 |
|148    |2203451|3     |2005-02-08|2001|Sweet November|0.6854845  |
|148    |2311863|3     |2005-06-08|2001|Sweet November|0.54871476 |
|148    |2358799|3     |2005-07-14|2001|Sweet November|0.5243217  |
|148    |2372856|2     |2005-07-28|2001|Sweet November|0.72899747 |
|148    |2373271|1     |2002-08-12|2001|Sweet November|0.19689558 |
|148    |2379533|4     |2005-06-05|2001|Sweet November|0.21744226 |
|148    |159421 |4     |2002-12-04|2001|Sweet November|0.17653401 |
|148    |616921 |2     |2005-08-05|2001|Sweet November|0.36853918 |
|148    |870375 |4     |2003-11-17|2001|Sweet November|0.6558877  |
|148    |901211 |2     |2005-02-05|2001|Sweet November|0.45227227 |
|148    |1028463|3     |2003-07-23|2001|Sweet November|0.8804474  |
|148    |1117838|1     |2004-07-05|2001|Sweet November|0.21820155 |
|148    |1135956|3     |2005-10-31|2001|Sweet November|0.46406838 |
|148    |1233833|4     |2003-09-05|2001|Sweet November|0.5981246  |
|148    |1325460|5     |2005-01-19|2001|Sweet November|0.004129283|
|148    |1436116|2     |2005-07-26|2001|Sweet November|0.35043898 |
|148    |1632860|4     |2005-02-07|2001|Sweet November|0.13690113 |
|148    |1712855|2     |2005-05-24|2001|Sweet November|0.82569    |
|148    |1849621|4     |2003-03-04|2001|Sweet November|0.583847   |
|148    |1852040|1     |2004-05-27|2001|Sweet November|0.82050574 |
|148    |2129024|3     |2005-02-28|2001|Sweet November|0.551252   |
|148    |2600937|3     |2004-07-26|2001|Sweet November|0.02661759 |
|148    |103767 |4     |2005-06-08|2001|Sweet November|0.54689085 |
|148    |120315 |4     |2005-06-05|2001|Sweet November|0.53565836 |
|148    |264596 |3     |2005-02-23|2001|Sweet November|0.66364217 |
|148    |326006 |3     |2003-11-26|2001|Sweet November|0.68057764 |
|148    |415279 |2     |2004-03-07|2001|Sweet November|0.31661683 |
|148    |491907 |4     |2005-06-16|2001|Sweet November|0.44043288 |
|148    |705714 |3     |2002-02-22|2001|Sweet November|0.53430027 |
|148    |788347 |4     |2005-02-14|2001|Sweet November|0.22639108 |
|148    |948485 |4     |2004-09-30|2001|Sweet November|0.05346503 |
|148    |1133611|1     |2002-07-19|2001|Sweet November|0.44367376 |
|148    |1239545|3     |2005-12-12|2001|Sweet November|0.63531166 |
|148    |1326341|3     |2004-04-30|2001|Sweet November|0.5571951  |
|148    |1612700|2     |2005-02-07|2001|Sweet November|0.2979436  |
|148    |1629176|3     |2004-04-28|2001|Sweet November|0.73170304 |
|148    |1820957|1     |2005-01-03|2001|Sweet November|0.36817443 |
|148    |1973956|4     |2005-04-26|2001|Sweet November|0.7151636  |
|148    |2001292|3     |2002-10-23|2001|Sweet November|0.49093994 |
|148    |2009638|4     |2003-12-29|2001|Sweet November|0.5339022  |
|148    |2051520|2     |2004-03-04|2001|Sweet November|0.10397753 |
|148    |2124267|3     |2004-06-02|2001|Sweet November|0.6773307  |
|148    |2160866|3     |2005-06-23|2001|Sweet November|0.56250244 |
|148    |2170295|1     |2003-09-26|2001|Sweet November|0.58104664 |
|148    |2201903|3     |2005-08-24|2001|Sweet November|0.43268225 |
|148    |2205323|4     |2003-12-31|2001|Sweet November|0.84442836 |
|148    |2347033|5     |2002-02-19|2001|Sweet November|0.31855056 |
|148    |2354764|2     |2002-01-05|2001|Sweet November|0.84537977 |
|148    |2361318|2     |2002-07-23|2001|Sweet November|0.7661425  |
|148    |2627190|2     |2005-02-23|2001|Sweet November|0.7297887  |
|148    |481158 |5     |2005-04-25|2001|Sweet November|0.01614587 |
|148    |589025 |3     |2005-02-14|2001|Sweet November|0.014197666|
|148    |649264 |3     |2004-02-05|2001|Sweet November|0.47007433 |
|148    |658412 |4     |2005-01-31|2001|Sweet November|0.67734617 |
|148    |671523 |5     |2005-01-13|2001|Sweet November|0.43235084 |
|148    |778841 |3     |2004-11-29|2001|Sweet November|0.45749998 |
|148    |780936 |5     |2005-05-27|2001|Sweet November|0.60383767 |
|148    |941092 |5     |2004-12-20|2001|Sweet November|0.6772521  |
|148    |1079625|3     |2005-09-13|2001|Sweet November|0.38666356 |
|148    |1250320|5     |2005-02-03|2001|Sweet November|0.7535052  |
|148    |1308212|3     |2003-12-08|2001|Sweet November|0.72960174 |
|148    |1384810|3     |2004-12-28|2001|Sweet November|0.67311424 |
|148    |1445378|3     |2004-02-07|2001|Sweet November|0.48819417 |
|148    |1485543|3     |2005-02-26|2001|Sweet November|0.34910187 |
|148    |1665761|5     |2005-08-17|2001|Sweet November|0.22273682 |
|148    |1796497|3     |2004-02-12|2001|Sweet November|0.40975258 |
|148    |1837290|3     |2003-09-04|2001|Sweet November|0.62777036 |
|148    |1935856|3     |2005-01-27|2001|Sweet November|0.82517767 |
|148    |2001326|3     |2005-01-30|2001|Sweet November|0.883636   |
|148    |2066772|2     |2003-01-25|2001|Sweet November|0.46496165 |
|148    |2462418|3     |2003-07-28|2001|Sweet November|0.56230927 |
|148    |2468821|2     |2004-10-01|2001|Sweet November|0.8266753  |
|148    |41068  |2     |2003-07-08|2001|Sweet November|0.42283556 |
|148    |125801 |4     |2005-01-29|2001|Sweet November|0.3595626  |
|148    |292643 |3     |2005-04-29|2001|Sweet November|0.35651225 |
|148    |461110 |4     |2003-10-20|2001|Sweet November|0.68494725 |
|148    |884926 |5     |2005-12-22|2001|Sweet November|0.30557677 |
|148    |903779 |2     |2001-08-29|2001|Sweet November|0.46974787 |
|148    |1360550|5     |2004-05-30|2001|Sweet November|0.98324573 |
|148    |1366054|5     |2002-09-09|2001|Sweet November|0.70275825 |
|148    |1392880|3     |2002-10-18|2001|Sweet November|0.51231277 |
|148    |1568498|5     |2002-12-08|2001|Sweet November|0.763676   |
|148    |2040324|2     |2004-07-06|2001|Sweet November|0.5179147  |
|148    |2086903|5     |2004-06-04|2001|Sweet November|0.6797656  |
|148    |2344810|2     |2003-12-29|2001|Sweet November|0.34365037 |
|148    |2365510|3     |2005-05-28|2001|Sweet November|0.58609164 |
|148    |2400698|3     |2005-06-28|2001|Sweet November|0.39149332 |
|148    |2437427|4     |2005-09-06|2001|Sweet November|0.36735567 |
|148    |2540418|4     |2005-06-13|2001|Sweet November|0.39489487 |
|148    |53872  |4     |2004-04-21|2001|Sweet November|0.64356935 |
|148    |91502  |4     |2005-08-09|2001|Sweet November|0.57917255 |
|148    |112312 |4     |2003-07-28|2001|Sweet November|0.30969557 |
|148    |115498 |2     |2001-12-02|2001|Sweet November|0.12595157 |
|148    |391893 |4     |2002-01-28|2001|Sweet November|0.48247772 |
|148    |425560 |3     |2001-12-24|2001|Sweet November|0.31509683 |
|148    |554587 |3     |2002-04-12|2001|Sweet November|0.80897045 |
|148    |857076 |5     |2005-09-29|2001|Sweet November|0.4150964  |
|148    |944421 |3     |2005-04-02|2001|Sweet November|0.19999105 |
|148    |1012934|4     |2004-10-19|2001|Sweet November|0.6571013  |
|148    |1116377|3     |2005-11-20|2001|Sweet November|0.43949717 |
|148    |1206822|3     |2004-12-09|2001|Sweet November|0.47536364 |
|148    |1297578|5     |2004-04-24|2001|Sweet November|0.46119857 |
|148    |1303814|4     |2005-10-05|2001|Sweet November|0.38400772 |
|148    |1558641|2     |2005-07-29|2001|Sweet November|0.23515198 |
|148    |1648869|4     |2005-05-23|2001|Sweet November|0.8902564  |
|148    |1672007|3     |2005-07-23|2001|Sweet November|0.3124981  |
|148    |1688478|3     |2005-07-06|2001|Sweet November|0.48038593 |
|148    |1805448|4     |2002-10-04|2001|Sweet November|0.74355775 |
|148    |1902870|4     |2005-08-12|2001|Sweet November|0.30535647 |
|148    |2413272|1     |2005-09-22|2001|Sweet November|0.4178847  |
|148    |2478777|2     |2004-08-03|2001|Sweet November|0.38524902 |
|148    |290248 |4     |2004-04-06|2001|Sweet November|0.5300354  |
|148    |466249 |4     |2004-10-07|2001|Sweet November|0.53038466 |
|148    |638108 |2     |2004-05-01|2001|Sweet November|0.37131053 |
|148    |668404 |3     |2004-08-11|2001|Sweet November|0.57807064 |
|148    |721603 |3     |2004-07-16|2001|Sweet November|0.4795114  |
|148    |980993 |2     |2001-08-02|2001|Sweet November|0.5718062  |
|148    |1018928|4     |2005-05-29|2001|Sweet November|0.76444685 |
|148    |1270057|5     |2005-09-01|2001|Sweet November|0.55027664 |
|148    |1464011|4     |2004-11-30|2001|Sweet November|0.81619775 |
|148    |1582137|4     |2002-08-13|2001|Sweet November|0.29261523 |
|148    |1613652|5     |2005-08-07|2001|Sweet November|0.18526845 |
|148    |1623631|4     |2001-08-31|2001|Sweet November|0.1319543  |
|148    |1870353|3     |2005-05-18|2001|Sweet November|0.25765693 |
|148    |2107396|3     |2004-09-05|2001|Sweet November|0.024143513|
|148    |2125368|4     |2003-12-29|2001|Sweet November|0.9310414  |
|148    |2198694|3     |2005-08-12|2001|Sweet November|0.20425212 |
|148    |2320307|3     |2001-11-26|2001|Sweet November|0.35604396 |
|148    |2566318|4     |2004-08-23|2001|Sweet November|0.7005188  |
|148    |2633546|5     |2005-01-06|2001|Sweet November|0.6207938  |
|148    |10947  |1     |2003-07-20|2001|Sweet November|0.78486425 |
|148    |217004 |4     |2005-11-06|2001|Sweet November|0.39417198 |
|148    |257685 |4     |2003-11-22|2001|Sweet November|0.32398415 |
|148    |378636 |4     |2005-11-04|2001|Sweet November|0.5177904  |
|148    |478389 |4     |2005-08-27|2001|Sweet November|0.09710124 |
|148    |510160 |3     |2005-07-30|2001|Sweet November|0.5114675  |
|148    |561184 |1     |2004-06-01|2001|Sweet November|0.7551528  |
|148    |569506 |3     |2002-12-16|2001|Sweet November|0.20318618 |
|148    |630205 |3     |2002-12-31|2001|Sweet November|0.21308403 |
|148    |630964 |4     |2005-06-18|2001|Sweet November|0.5852565  |
|148    |686510 |2     |2003-12-02|2001|Sweet November|0.8302514  |
|148    |704538 |4     |2002-03-09|2001|Sweet November|0.24449635 |
|148    |1018547|3     |2005-09-17|2001|Sweet November|0.7161098  |
|148    |1070554|5     |2004-11-29|2001|Sweet November|0.8308247  |
|148    |1193629|3     |2004-10-01|2001|Sweet November|0.8583989  |
|148    |1370564|4     |2003-04-01|2001|Sweet November|0.76273626 |
|148    |1478244|3     |2002-02-25|2001|Sweet November|0.72463876 |
|148    |1520998|1     |2002-01-10|2001|Sweet November|0.24048789 |
|148    |1590902|2     |2003-11-11|2001|Sweet November|0.45868233 |
|148    |1599243|1     |2003-09-05|2001|Sweet November|0.599483   |
|148    |1614609|4     |2004-10-09|2001|Sweet November|0.29774407 |
|148    |1687386|4     |2002-04-05|2001|Sweet November|0.5130493  |
|148    |2214406|4     |2005-02-07|2001|Sweet November|0.77534723 |
|148    |2339243|3     |2005-02-03|2001|Sweet November|0.49522865 |
|148    |2402063|3     |2004-02-15|2001|Sweet November|0.052003387|
|148    |2632159|4     |2004-05-20|2001|Sweet November|0.56010306 |
|148    |357056 |4     |2005-06-23|2001|Sweet November|0.79187846 |
|148    |421901 |3     |2002-07-29|2001|Sweet November|0.63945156 |
|148    |428296 |4     |2004-02-10|2001|Sweet November|0.11348418 |
|148    |710933 |5     |2001-08-03|2001|Sweet November|0.558457   |
|148    |903193 |2     |2005-06-05|2001|Sweet November|0.18803465 |
|148    |940024 |5     |2003-04-10|2001|Sweet November|0.36154324 |
|148    |1008300|3     |2004-04-20|2001|Sweet November|0.4719554  |
|148    |1015526|3     |2002-08-16|2001|Sweet November|0.29661003 |
|148    |1335797|3     |2004-06-16|2001|Sweet November|0.47641572 |
|148    |1376007|3     |2005-03-28|2001|Sweet November|0.39626893 |
|148    |1470710|4     |2001-12-29|2001|Sweet November|0.7720623  |
|148    |1527104|5     |2005-01-25|2001|Sweet November|0.17734534 |
|148    |1661999|5     |2005-08-01|2001|Sweet November|0.71313256 |
|148    |1694373|5     |2004-11-29|2001|Sweet November|0.5454002  |
|148    |2047522|2     |2005-02-16|2001|Sweet November|0.34886807 |
|148    |2115578|5     |2005-10-02|2001|Sweet November|0.3959279  |
|148    |2261521|5     |2005-02-01|2001|Sweet November|0.58797514 |
|148    |2263440|5     |2005-04-19|2001|Sweet November|0.46356615 |
|148    |2282493|2     |2005-09-06|2001|Sweet November|0.5508559  |
|148    |2296586|3     |2004-04-29|2001|Sweet November|0.6256032  |
|148    |2378867|4     |2005-01-06|2001|Sweet November|0.7360553  |
|148    |2480676|3     |2005-04-12|2001|Sweet November|0.6079232  |
|148    |2502469|4     |2005-10-23|2001|Sweet November|0.3158493  |
|148    |2556791|4     |2005-05-31|2001|Sweet November|0.35071135 |
|148    |2592716|4     |2005-02-24|2001|Sweet November|0.76472473 |
|148    |2622511|2     |2002-04-01|2001|Sweet November|0.5752946  |
|148    |195059 |2     |2004-08-01|2001|Sweet November|0.2671168  |
|148    |503051 |1     |2005-05-28|2001|Sweet November|0.6091112  |
+-------+-------+------+----------+----+--------------+-----------+
only showing top 200 rows

</code></pre>