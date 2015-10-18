__author__ = 'Eva Sharma and Praveen Kumar'

"""
 Read the data from the files.
 Create a dictionary :
    key : twitter user id
    value: date : twitter id

 Another dictionary:
    key : tweet id
    value : tweet

Another dictionary:
    key : twitter user id
    value : location : array : city, state
    60730027

"""
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import random
import os

DEBUG = True

BASE_DIR = os.path.dirname(__file__)
trainingSetTweetsFilePath = os.path.join(BASE_DIR,"../data/training_set_tweets.txt")
trainingSetUsersFilePath = os.path.join(BASE_DIR,"../data/training_set_users.txt")
testSetTweetsFilePath = os.path.join(BASE_DIR,"../data/test_set_tweets.txt")
testSetUsersFilePath = os.path.join(BASE_DIR,"../data/test_set_users.txt")


userToTweetsMap = dict()
tweets = dict()
twitterUsers = dict()

def populateTweets(tweetId,tweet):
    """
    populate the dictionary tweets with key as tweet id and value as the tweet
    """
    tweets[tweetId] = tweet

def populateTwitterUsers(userId,location):
    """
    populate the dictionary twitterUsers with key as twitter user id
    and value as array having index 0 as the state and index 1 as the city
    """
    twitterUsers[userId] = location.split(",")
    if(len(twitterUsers[userId]) > 1):
        twitterUsers[userId][1] = twitterUsers[userId][1].strip()


def populateUserToTweetsMap(userId,date,tweetId):
    """
    Dictionary :
    key : user_id
    value:  Dict(date : tweet_id)
    """
    if not userId in userToTweetsMap:
        userToTweetsMap[userId] = dict()
        userToTweetsMap[userId][date] = [tweetId]
    else:
        if userToTweetsMap[userId].get(date, None) is not None:
            userToTweetsMap[userId][date].append(tweetId)
        else:
            userToTweetsMap[userId][date] = [tweetId]


def readUsersFile(users_file):
    f = open(users_file,'r')
    for line in f:
        userId = line.strip("\n").split("\t")[0]
        location = line.strip("\n").split("\t")[1]
        populateTwitterUsers(userId,location)
    f.close()

def readTweetsFile(file):
    f = open(file,'r')
    for line in f:
        if not line in ['\n', '\r\n']:
            tweetData = line.strip("\n").split("\t")
            if(len(tweetData) == 4):
                userId = tweetData[0]
                tweetId = tweetData[1]
                tweet = tweetData[2]
                dateTime = tweetData[3].split()[0] if tweetData[3].strip()!='' else tweetData[3]
            else:
                continue
            if (userId != '' and tweetId != '' and tweet != '' and datetime != ''):
                populateTweets(tweetId,tweet)
                populateUserToTweetsMap(userId,dateTime,tweetId)
    f.close()

def writeToFile(file,obj):
    f = open(file,'w')
    json.dump(obj,f)

def sample_users(n):
    return random.sample(twitterUsers.keys(), n)


def getUsersGoingInactiveAfterMonth(month, year):
    """
    Returns the list of users who were active in given month but not after that
    """
    print "here"
    start_date = datetime(year, month, 1)
    end_date = start_date + relativedelta(months=1)
    result_users = []
    for user, tweets in userToTweetsMap.iteritems():
        tweet_dates = map(str_to_date, tweets.keys())
        last_tweet_date = max(tweet_dates)
        if last_tweet_date >= start_date and last_tweet_date < end_date:
            result_users.append(user)
            # print tweets.keys(), map(len, tweets.values()), getNumMonthsActive(tweet_dates)
            if DEBUG and len(result_users) > 10000:
                return result_users
    return result_users

def str_to_date(date_str):
    if isinstance(date_str, datetime):
        return date_str
    try:
        d = datetime.strptime(date_str, '%Y-%m-%d')
        return d
    except ValueError:
        return datetime.utcfromtimestamp(0)

def getNumMonthsActive(dates):
    months = set()
    for date in dates:
        if date > datetime.utcfromtimestamp(0):
            months.add(date.month)
    return len(months)

def tweetFrequency(users):
    freq_data = dict()
    for user in users:
        tweets = userToTweetsMap.get(user, None)
        if tweets is None:
            freq_data[user] = (0, 0, 0)
        else:
            tweet_dates = map(str_to_date, tweets.keys())
            first_date = min(tweet_dates)
            last_date = max(tweet_dates)
            num_days_tweeted = len(tweet_dates)
            date_range_active = (last_date - first_date).days + 1
            num_tweets = 0
            for t in tweets.values():
                num_tweets += len(t)
            freq_data[user] = (date_range_active, num_days_tweeted, num_tweets)
    return freq_data


def read_raw_data():
    """
    Parse original dataset
    """
    print("Reading ",trainingSetUsersFilePath)
    readUsersFile(trainingSetUsersFilePath)
    print("\nReading ",trainingSetTweetsFilePath)
    readTweetsFile(trainingSetTweetsFilePath)
    print("\nWriting To files")
    writeToFile(os.path.join(BASE_DIR,"Dict/twitterUsers.txt"),twitterUsers)
    writeToFile(os.path.join(BASE_DIR,"Dict/tweets.txt"),tweets)
    writeToFile(os.path.join(BASE_DIR,"Dict/userToTweetsMap.txt"),userToTweetsMap)

def read_parsed_data():
    """
    Read parsed data from files
    """
    global twitterUsers
    global tweets
    global userToTweetsMap
    print("\nPopulating twitterUsers")
    twitterUsers = json.load(open(os.path.join(BASE_DIR,"Dict/twitterUsers.txt")))
    #print("\nPopulating tweets")
    #tweets = json.load(open(os.path.join(BASE_DIR,"Dict/tweets.txt")))
    print("\nPopulating userToTweetsMap")
    userToTweetsMap = json.load(open(os.path.join(BASE_DIR,"Dict/userToTweetsMap.txt")))
    print("Done populating data")

if __name__ == '__main__':
    re_read = False
    if re_read:
        read_raw_data()
    read_parsed_data()
    print len(twitterUsers)
    nov_quit_users = getUsersGoingInactiveAfterMonth(11, 2009)
    random_users = sample_users(10000)
    nov_quit_users_freq = tweetFrequency(nov_quit_users)
    random_users_freq = tweetFrequency(random_users)

    avg_tweet_per_day = 0
    for (rng, td, nt) in nov_quit_users_freq.values():
        avg_tweet_per_day += 1.0*nt/td
    print avg_tweet_per_day/len(nov_quit_users)

    print len(nov_quit_users)
    avg_tweet_per_day = 0
    for (rng, td, nt) in tweetFrequency(random_users).values():
        if (rng > 0):
            avg_tweet_per_day += 1.0*nt/td
    print avg_tweet_per_day/len(random_users)
