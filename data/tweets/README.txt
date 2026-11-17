Tweets: input of twitter_1 and twitter_2
========================================

The two Twitter dataflows read one file, data/tweets/twitter.txt: the real tweets of the paper (100,000 tweets
of a crypto-related scrape, 91 days, 5.3 MB), anonymized (see below). It is included in this artifact; with SMALL=1
they read its first 10% (data/small/tweets/twitter.txt, made by data/make_small.py). The raw scrape
(tweets_hashtags_100K.csv, 457 MB) is not included.

  twitter.txt                    the data (the format is documented below)
  README.txt                     this file: the exact input interface of the workflows
  make_twitter_txt.py            how twitter.txt was derived from the raw scrape (documents the preprocessing)
  stats_tweets.py                prints statistics of the file (used to pick the window sizes)

Your own tweets go to data/tweets/twitter.txt in the format below; nothing else in the artifact has to change
(the window sizes, the sampling probabilities and the expected output counts of experiments/common.sh are those
of the real file, tune SAMPLE_PROB / FULL_OUTPUTS for a different input).

Record schema (input interface of the workflows)
------------------------------------------------
One tweet per line, UTF-8, no header, exactly 8 comma-separated columns (a tweet is split with at most 8
parts, so the last column may contain anything but must not contain a newline):

  #  column          type    meaning
  1  tweetId         int64   unique id of the record (the line number in the published file; the Twitter id is
                             removed); parsed as a long, otherwise unused
  2  timestamp       int64   creation time, UTC epoch milliseconds: the event time of the tweet
  3  hashtags        string  hashtags of the tweet separated by '|', no duplicates within a tweet, empty
                             if the tweet has none (about 12% of the real tweets); '|' and ',' never occur
                             inside a hashtag. The key of both workflows is one hashtag
  4  likes           int32   like count, >= 0
  5  retweets        int32   retweet count, >= 0
  6  isVerified      bool    "true" or "false": legacy verified flag of the author (always "false" in
                             the real data)
  7  isBlueVerified  bool    "true" or "false": blue-check flag of the author
  8  location        string  "1" if the author's profile has a location, empty if not (the free text is
                             removed). The workflows only test for presence

Example: 2,1723516530000,macro|us_inflation,3,1,false,true,

What the workflows compute from the columns:
  twitter_1  one record per (tweet, hashtag): key = hashtag, score = likes + retweets,
             verified = isVerified OR isBlueVerified; average score per hashtag over 30 min windows sliding
             5 min, for all tweets and for verified tweets only, joined on the hashtag (30 min / 5 min)
  twitter_2  one record per (tweet, hashtag): average score per hashtag over 6 h windows sliding 1 h,
             counted separately for tweets with and without location (hasLocation = location != "")
Tweets without hashtags produce no records. The columns tweetId and the location text do not influence
any output.

Ordering requirement
--------------------
The file must be sorted by timestamp, non-decreasing: the sources emit periodic watermarks (every millisecond
of processing time by default, CAPS_WM_INTERVAL_MS) with no out-of-orderness (CAPS_DELTA_MS = 0), so a tweet
earlier than its predecessor would be late data. The real file has distinct timestamps
(see below), which also makes the tuples pairwise distinct for the provenance operators, so a file of your
own should have strictly increasing timestamps too.

How twitter.txt was derived (make_twitter_txt.py)
-------------------------------------------------
Input: tweets_hashtags_100K.csv, a raw scrape with one row per tweet; the columns used are id, createdatutc
("YYYY-MM-DD HH:MM:SS", second precision), entities (JSON with the hashtag list), likecount, retweetcount and
author (JSON with isVerified, isBlueVerified, location).
  1. timestamp = createdatutc interpreted as UTC, in epoch milliseconds
  2. hashtags = the texts of entities.hashtags, deduplicated in order (the scrape lists a hashtag twice for
     about 2.6% of the tweets), joined with '|'
  3. likes, retweets = likecount, retweetcount as integers (empty means 0)
  4. isVerified, isBlueVerified, location = from author; commas in the location become spaces
  5. rows are sorted by timestamp; the raw data has second precision, so ties are common (about 3.3K shared
     timestamps): every tie is bumped by +1 ms over its predecessor, which keeps the order and the second
Result: 100,000 rows over 91 days, 176,912 hashtag occurrences of 19,113 distinct
hashtags, about 89% verified (all of it blue verification), about 63% with a location.

Anonymization
-------------
The published file is anonymized: the tweet id is replaced by the line number, the profile location by its
presence ("1" or empty), and all timestamps are shifted by one fixed offset. The text, the authors and every other
field of the raw scrape are removed. The workflows use neither the id nor the location text, and every window is
aligned to whole hours or days, so all outputs of all methods are the same as on the original timestamps (checked
with experiments/verify on twitter_1 and twitter_2, 20.5M outputs). The per-output query benchmarks sample outputs by
a hash that includes the timestamp, so they time a different sample of about 1,000 outputs than the paper's runs.
