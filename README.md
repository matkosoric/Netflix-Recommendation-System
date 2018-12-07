# Netflix Recommendation System

![Netflix logo - Matko Soric](https://raw.githubusercontent.com/matkosoric/Netflix-Recommendation-System/master/src/main/resources/images/netflix-logo.png?raw=true "Netflix logo - Matko Soric")


Netflix prize was an open competition for the best collaborative filtering algorithm, which started in 2006.
BellKor's Pragmatic Chaos team from AT&T Labs won the prize back in 2009.
This Spark application will user Spark's 2.4 built-in ALS algorithm to create a recommendation model for the data set from the competition.

Preprocessing1.scala will create a four-column dataframe (movieId, userId, rating, date) from the original data and write it to the intermediate folder.
Preprocessing2.scala will use data from the intermediate step and write it in a snappy compressed parquet format.
Exploring.scala prints some sample data and general info about used data.
ProbeParser.scala and QualifyingParser.scala are scripts for transforming original logs to Spark-friendly csv tabular structure.
Training.scala will fit ALS model to the training set using k-fold validation and a hyper-parameter grid with 64 different values.
Predicting.scala loads trained ALS model, calculates predicted values on the data from the original probe.txt and evaluates model's RMSE.

Trained ALS model is located under /src/main/resources/model. It's RMSE is 0.8904.
I withheld preprocessed data from the intermediate steps in accordance with Netflix's official instruction not to redistribute the data.

Next steps would be to use model on qualifying data set, or to enrich data with additional features, like genre.

[Netflix prize - Official page](https://www.netflixprize.com/)

[Netflix prize - Wiki](https://en.wikipedia.org/wiki/Netflix_Prize)

[Netflix Prize - Slides](http://courses.washington.edu/css581/lecture_slides/09a_Netflix_Prize.pdf)


### Dataset

Netflix does not provide access to the original data set, probably due to the legal issues. Nonetheless, it can be downloaded from the archived UCI ML repository:
[Netflix Prize Data Set](https://web.archive.org/web/20090925184737/http://archive.ics.uci.edu/ml/datasets/Netflix+Prize)


### Tools

[Spark 2.4.0 MLlib](https://spark.apache.org/docs/latest/ml-guide.html)


### Results

##### RMSE =  0.8904

Cores during the training process
![Training process - Matko Soric](https://raw.githubusercontent.com/matkosoric/Netflix-Recommendation-System/master/src/main/resources/images/training-screenshoot.png?raw=true "Training process - Matko Soric")

Model parameters:


	als_5c1c03ac0dc9-alpha: 0.6,
	als_5c1c03ac0dc9-checkpointInterval: 5,
	als_5c1c03ac0dc9-coldStartStrategy: drop,
	als_5c1c03ac0dc9-finalStorageLevel: MEMORY_AND_DISK,
	als_5c1c03ac0dc9-implicitPrefs: false,
	als_5c1c03ac0dc9-intermediateStorageLevel: MEMORY_AND_DISK,
	als_5c1c03ac0dc9-itemCol: movieId,
	als_5c1c03ac0dc9-maxIter: 10,
	als_5c1c03ac0dc9-nonnegative: false,
	als_5c1c03ac0dc9-numItemBlocks: 10,
	als_5c1c03ac0dc9-numUserBlocks: 10,
	als_5c1c03ac0dc9-predictionCol: prediction,
	als_5c1c03ac0dc9-rank: 4,
	als_5c1c03ac0dc9-ratingCol: rating,
	als_5c1c03ac0dc9-regParam: 0.1,
	als_5c1c03ac0dc9-seed: 555,
	als_5c1c03ac0dc9-userCol: userId


Data exploration output:

<pre><code>
Sample data: 
+-------+-------+------+----------+----+-------------------------------------------+
|movieId|userId |rating|date      |year|title                                      |
+-------+-------+------+----------+----+-------------------------------------------+
|15129  |2542878|4     |2003-12-05|1939|Mr. Smith Goes to Washington               |
|15425  |2069526|5     |2004-02-29|1998|The Big Lebowski                           |
|15755  |958660 |5     |2004-07-21|1988|Big                                        |
|7234   |500873 |3     |2005-10-29|2000|Men of Honor                               |
|4841   |2301393|3     |2004-06-19|1995|Judge Dredd                                |
|13865  |528367 |3     |2004-07-08|1998|Mercury Rising                             |
|1962   |196414 |5     |2004-09-23|2004|50 First Dates                             |
|12435  |2189558|4     |2005-07-16|1989|Lethal Weapon 2                            |
|9728   |2051858|3     |2004-02-14|1997|As Good as It Gets                         |
|1406   |697734 |4     |2005-01-10|1991|Hook                                       |
|1467   |1592890|5     |2005-07-21|1993|Three Musketeers                           |
|2152   |1552265|1     |2004-03-04|2000|What Women Want                            |
|2430   |2464580|5     |2005-08-10|1979|Alien: Collector's Edition                 |
|9232   |914338 |3     |2004-07-16|2003|Boat Trip                                  |
|3282   |611924 |4     |2005-06-06|2004|Sideways                                   |
|11490  |845597 |3     |2005-03-24|1992|A League of Their Own                      |
|2235   |1068610|3     |2005-05-02|2004|Undertow                                   |
|175    |185950 |5     |2005-02-23|1992|Reservoir Dogs                             |
|15952  |1060658|4     |2004-12-05|2000|The Replacements                           |
|14928  |1722103|2     |2005-08-15|1989|Dead Poets Society                         |
|12605  |1189649|4     |2005-05-23|1978|National Lampoon's Animal House            |
|10282  |504981 |3     |2004-11-11|1998|Godzilla                                   |
|3701   |671319 |3     |2005-06-27|1944|The Fighting Seabees                       |
|1180   |2343813|5     |2002-07-18|2001|A Beautiful Mind                           |
|8764   |1346514|5     |2005-09-05|1996|Happy Gilmore                              |
|12155  |1053802|4     |2004-12-16|2004|Spider-Man 2                               |
|3826   |2521098|5     |2005-09-04|1999|Music of the Heart                         |
|1659   |2627984|4     |2005-10-20|1993|Grumpy Old Men                             |
|16139  |1456244|3     |2004-01-28|1991|Father of the Bride                        |
|10895  |1261931|3     |2003-09-02|1969|Cactus Flower                              |
|3046   |2053064|3     |2005-02-14|1990|The Simpsons: Treehouse of Horror          |
|7240   |1272495|2     |2005-12-29|1987|Beverly Hills Cop II                       |
|9243   |2327536|4     |2005-07-15|1991|Days of Being Wild                         |
|7055   |2645587|2     |2003-05-05|1995|Get Shorty                                 |
|11661  |1233877|3     |2005-04-04|1984|Friday the 13th: Part 4: The Final Chapter |
|10451  |1161994|5     |2005-08-10|1971|A Clockwork Orange                         |
|14691  |355756 |5     |2003-03-29|1999|The Matrix                                 |
|8687   |2452947|3     |2003-07-27|2002|Star Wars: Episode II: Attack of the Clones|
|8743   |1688893|3     |2003-07-31|2002|Ice Age                                    |
|14646  |1978009|4     |2004-04-01|1989|Pet Sematary                               |
+-------+-------+------+----------+----+-------------------------------------------+
only showing top 40 rows

In our complete dataset we have 100480507 reviews, performed by 480189 users, on a collection of 17770 movies. 

Standard data set statistics:
+-------+-----------------+------------------+-----------------+------------------+--------------------+
|summary|          movieId|            userId|           rating|              year|               title|
+-------+-----------------+------------------+-----------------+------------------+--------------------+
|  count|        100479542|         100479542|        100479542|         100479542|           100479542|
|   mean| 9070.90042558116|1322488.8069149738|3.604298027154622|1993.9115270947393|            Infinity|
| stddev|5131.885554180567| 764536.6971016071| 1.08521509230125|12.400832155531443|                 NaN|
|    min|                1|                 6|                1|              1896|'Allo 'Allo!: Ser...|
|    max|            17770|           2649429|                5|              2005|                 sex|
+-------+-----------------+------------------+-----------------+------------------+--------------------+

Top 20 movies by average score, with minimum and maximum score, and number of reviews:
+---------------------------------------------------------------------------+--------+--------+------------+----------+
|title                                                                      |minScore|maxScore|averageScore|numReviews|
+---------------------------------------------------------------------------+--------+--------+------------+----------+
|Lord of the Rings: The Return of the King: Extended Edition                |1       |5       |4.723       |73335     |
|The Lord of the Rings: The Fellowship of the Ring: Extended Edition        |1       |5       |4.717       |73422     |
|Lord of the Rings: The Two Towers: Extended Edition                        |1       |5       |4.703       |74912     |
|Lost: Season 1                                                             |1       |5       |4.671       |7249      |
|Battlestar Galactica: Season 1                                             |1       |5       |4.639       |1747      |
|Fullmetal Alchemist                                                        |1       |5       |4.605       |1633      |
|Trailer Park Boys: Season 3                                                |1       |5       |4.6         |75        |
|Trailer Park Boys: Season 4                                                |1       |5       |4.6         |25        |
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
+---------------------------------------------------------------------------+--------+--------+------------+----------+
only showing top 20 rows

Twenty movies with the smallest number of reviews: 
+--------------------------------------------------+----------+
|title                                             |numReviews|
+--------------------------------------------------+----------+
|Mobsters and Mormons                              |3         |
|The Land Before Time IV: Journey Through the Mists|5         |
|Hockey Mom                                        |10        |
|Larryboy and the Rumor Weed                       |10        |
|Dune: Extended Edition                            |13        |
|The Land Before Time VI: The Secret of Saurus Rock|14        |
|The Triangle                                      |22        |
|Journey Into Amazing Caves: IMAX                  |22        |
|Ah! My Goddess                                    |23        |
|Trailer Park Boys: Season 4                       |25        |
|Inspector Morse 33: The Remorseful Day            |26        |
|Love on Lay-Away                                  |27        |
|My Wife's Murder                                  |29        |
|Invasion: Earth                                   |31        |
|Blood and Black Lace                              |33        |
|Inspector Morse 6: The Settling of the Sun        |34        |
|Bleak House                                       |35        |
|Danielle Steel's Remembrance                      |36        |
|Danielle Steel's Once in a Lifetime               |36        |
|Bram Stoker's: To Die For                         |36        |
+--------------------------------------------------+----------+
only showing top 20 rows

Five users with the smallest number of ratings:
+-------+---------------+
|userId |numberOfReviews|
+-------+---------------+
|1751413|1              |
|2287107|1              |
|821585 |1              |
|167813 |1              |
|562162 |1              |
+-------+---------------+

Five users with the largest number of ratings:
+-------+---------------+
|userId |numberOfReviews|
+-------+---------------+
|305344 |17653          |
|387418 |17436          |
|2439493|16565          |
|1664010|15813          |
|2118461|14831          |
+-------+---------------+

Probe data set sample:
+-------+-------+
|movieId| userId|
+-------+-------+
|      1|1027056|
|  10001|2350428|
|  10024|2027932|
|  10036|2344026|
|   1004| 184574|
|  10042|1648015|
|  10042|1019149|
|  10042| 247386|
|  10044|2604177|
|  10044| 386568|
|  10053|2593690|
|  10064|  66853|
|   1008|2276942|
|  10080|1177965|
|  10080| 156747|
+-------+-------+
only showing top 15 rows

Qualifying data set sample:
+-------+-------+----------+
|movieId| userId|      date|
+-------+-------+----------+
|   1000|2246603|2005-05-15|
|   1001| 865955|2005-10-13|
|   1001|2260753|2005-12-14|
|  10010| 684951|2005-12-12|
|  10010|2075868|2005-11-01|
|  10019| 271472|2004-10-24|
|  10020|1436995|2005-06-26|
|  10022|2646684|2005-11-21|
|  10034|1884816|2005-04-01|
|  10036|2193771|2005-12-06|
|  10036|2132394|2005-10-28|
|  10042| 965634|2005-11-15|
|  10042|1485736|2005-11-20|
|  10042| 187484|2005-12-11|
|  10042|1166633|2005-11-26|
+-------+-------+----------+
only showing top 15 rows

+-------+-------+------+----------+----+------------------------------+----------+
|movieId|userId |rating|date      |year|title                         |prediction|
+-------+-------+------+----------+----+------------------------------+----------+
|1      |1027056|3     |2005-12-03|2003|Dinosaur Planet               |3.7297606 |
|3      |1148318|4     |2004-11-29|1997|Character                     |4.393401  |
|8      |2117067|5     |2005-11-08|2004|What the #$*! Do We Know!?    |3.2681036 |
|8      |2629028|5     |2005-12-26|2004|What the #$*! Do We Know!?    |3.4664326 |
|11     |2615605|2     |2005-12-23|1999|Full Frame: Documentary Shorts|3.2669764 |
|17     |451827 |2     |2005-12-28|2005|7 Seconds                     |2.6999297 |
|17     |2002136|3     |2005-10-22|2005|7 Seconds                     |3.0721776 |
|18     |491066 |5     |2005-12-02|1994|Immortal Beloved              |3.939982  |
|18     |1501608|4     |2005-11-09|1994|Immortal Beloved              |3.7334476 |
|28     |1710041|4     |2005-02-13|2002|Lilo and Stitch               |3.6916113 |
|28     |1880468|4     |2005-12-31|2002|Lilo and Stitch               |4.144219  |
|28     |1974539|4     |2005-12-28|2002|Lilo and Stitch               |3.5790312 |
|28     |1987695|4     |2005-09-01|2002|Lilo and Stitch               |3.3689344 |
|28     |2028350|3     |2004-08-26|2002|Lilo and Stitch               |4.257824  |
|28     |2499426|3     |2005-09-15|2002|Lilo and Stitch               |3.1333754 |
|30     |69212  |4     |2005-11-27|2003|Something's Gotta Give        |4.1014843 |
|30     |138733 |3     |2005-11-12|2003|Something's Gotta Give        |2.8050036 |
|30     |646202 |4     |2005-09-08|2003|Something's Gotta Give        |4.0335736 |
|30     |648214 |4     |2005-02-05|2003|Something's Gotta Give        |3.6525288 |
|30     |839205 |4     |2005-03-05|2003|Something's Gotta Give        |3.1067724 |
|30     |956887 |5     |2004-11-01|2003|Something's Gotta Give        |4.80021   |
|30     |1332244|4     |2005-05-31|2003|Something's Gotta Give        |3.898171  |
|30     |1359407|5     |2005-12-18|2003|Something's Gotta Give        |3.7634425 |
|30     |1371690|4     |2005-10-25|2003|Something's Gotta Give        |3.8809943 |
|30     |1716748|4     |2005-08-08|2003|Something's Gotta Give        |3.9752717 |
|30     |1971250|5     |2005-08-28|2003|Something's Gotta Give        |3.4830403 |
|30     |2165525|4     |2005-10-26|2003|Something's Gotta Give        |4.0227585 |
|30     |2209225|3     |2005-09-17|2003|Something's Gotta Give        |2.7385604 |
|30     |2209890|2     |2004-10-30|2003|Something's Gotta Give        |3.308076  |
|30     |2230206|5     |2005-03-08|2003|Something's Gotta Give        |3.7086098 |
|30     |2260825|4     |2004-07-22|2003|Something's Gotta Give        |4.2019987 |
|30     |2405098|3     |2005-11-08|2003|Something's Gotta Give        |3.8531399 |
|38     |1037555|1     |2004-05-24|2003|Daydream Obsession            |2.4396152 |
|44     |9625   |3     |2005-06-08|1996|Spitfire Grill                |4.0473433 |
|44     |26965  |1     |2005-12-01|1996|Spitfire Grill                |3.349677  |
|44     |1508206|3     |2005-09-15|1996|Spitfire Grill                |4.2703776 |
|46     |533295 |4     |2005-12-20|1964|Rudolph the Red-Nosed Reindeer|3.3534713 |
|46     |2414331|5     |2005-12-05|1964|Rudolph the Red-Nosed Reindeer|4.1098204 |
|52     |1060617|3     |2005-07-18|2002|The Weather Underground       |3.6908722 |
|55     |984021 |4     |2005-07-10|1995|Jade                          |3.81435   |
+-------+-------+------+----------+----+------------------------------+----------+
only showing top 40 rows


</code></pre>