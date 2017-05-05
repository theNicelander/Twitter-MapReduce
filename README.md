# Twitter-MapReduce
First coursework for Big Data Processing, QMUL 2016

Processing 25 million tweets from the Rio 2016 Olympics using using the Hadoop Framework. 

See Report.pdf for full details and code implementation.

# A. Content Analysis

![Length-petur-einarsson](https://github.com/Hunang/Twitter-MapReduce/blob/master/LengthHistogram.png "Length-petur-einarsson")

The histogram above shows the number of tweets in thousands by length, put forth during the 2016 Rio Olympics, using the hashtags #Rio2016 or #rioolympics. 
The maximum length of a tweet is 140 characters by default.

# B. Time Analysis

![Time-Series-petur-einarsson](https://github.com/Hunang/Twitter-MapReduce/blob/master/TimeSeries.png "Time-Series-petur-einarsson")

The time series shows the number of tweets in thousands, per day of the 2016 Rio Olympics.

The date in the dataset is stored as UNIX time, which is based on UTC time. The opening ceremony of the games started at 23:00 UTC, 5th of August and ended at 03:00 UTC, 6th of August. Many tweets were posted during the opening ceremony which is why we see a spike on the 6th of August.

We can also see a smaller spike on the 19th of August as that day many group sports such as hockey and badminton concluded, as well as Usain Bolt, a world famous runner from Jamaica, earned his 3rd gold during course of the games.

# C. Hashtag Analysis
This part of the report covers which countries got the most support during the course of the games.

![Country-Support-petur-einarsson](https://github.com/Hunang/Twitter-MapReduce/blob/master/Country-Support.png "Country-Support-petur-einarsson")

The two most supported countries are Brazil and USA respectively, which is consistent with results reported by others. This was to be expected as the games were held in Brazil as well as Twitter being a very popular social media platform in the USA. Canada received the most support per capita.

![Country-Support-petur-einarsson](https://github.com/Hunang/Twitter-MapReduce/blob/master/Country-Support2.png "Country-Support-petur-einarsson")

As the most supported countries are Brazil and USA, it’s logical that the top hashtags are related to them, with #BRA, #USA and #TEAMUSA being the most used hashtags related with country support. The most common forms of country support hashtags, are hashtags consisting of only the 3 digit country codes or combination the word “TEAM” and the three digit country codes. Exceptions being #VAMOSARGENTINA and #TEAMGB, which were hashtags marketed in their respective countries to support their teams in the Olympics.
