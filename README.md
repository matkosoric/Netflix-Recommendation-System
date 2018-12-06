# Netflix Recommendation System

![Netflix logo - Matko Soric](https://raw.githubusercontent.com/matkosoric/Netflix-Recommendation-System/master/src/main/resources/images/netflix-logo.png?raw=true "Netflix logo - Matko Soric")


Netflix prize was an open competition for the best collaborative filtering algorithm, which started in 2006.
It was won by BellKor's Pragmatic Chaos team from AT&T Labs won the prize back in 2009.
This Spark application will user Spark's built-in ALS algorithm to create a recommendation model for the data set from the competition.

Preprocessing1.scala will create a four-column dataframe (movieId, userId, rating, date) from the original data and write it to the intermediate folder.
Preprocessing2.scala will use data from the intermediate step and write it in a snappy compressed parquet format.


[Netflix prize](https://en.wikipedia.org/wiki/Netflix_Prize)

### Dataset

Netflix does not provides access to the original dataset, probably due to the legal issues. Nonetheless, it can be downloaded from the archived UCI ML repository:
[Netflix Prize Data Set](https://web.archive.org/web/20090925184737/http://archive.ics.uci.edu/ml/datasets/Netflix+Prize)


### Tools

[Spark 2.4.0 MLlib](https://spark.apache.org/docs/latest/ml-guide.html)



### Results

Cores during the training process
![Training process - Matko Soric](https://raw.githubusercontent.com/matkosoric/Netflix-Recommendation-System/master/src/main/resources/images/training-screenshoot.png?raw=true "Training process - Matko Soric")

Model parameters:

{
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
}

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

Twenty movies the the smallest number of reviews: 
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

</code></pre>