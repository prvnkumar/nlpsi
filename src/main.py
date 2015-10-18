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
from enum import Enum
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

class Ops(Enum):
    Eq  = '=='
    LtE  = '<='
    Lt  = '<'
    GtE  = '>='
    Gt  = '>'

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

def sampleUsers(n):
    return random.sample(twitterUsers.keys(), n)


def filterUsersByEndMonth(users, month, year):
    """
    Returns the list of users who were active in given month but not after that
    """
    startDate = datetime(year, month, 1)
    endDate = startDate + relativedelta(months=1)
    resultUsers = []
    for user in users:
        tweets = userToTweetsMap.get(user, None)
        if tweets == None:
            continue
        tweetDates = map(strToDate, tweets.keys())
        lastTweetDate = max(tweetDates)
        if lastTweetDate >= startDate and lastTweetDate < endDate:
            resultUsers.append(user)
            # print tweets.keys(), map(len, tweets.values()), getNumMonthsActive(tweetDates)
    return resultUsers

def strToDate(dateStr):
    if isinstance(dateStr, datetime):
        return dateStr
    try:
        d = datetime.strptime(dateStr, '%Y-%m-%d')
        return d
    except ValueError:
        return datetime.utcfromtimestamp(0)

def getNumMonthsActive(user):
    """
    Get number of months a user is active
    """
    dates = map(strToDate, userToTweetsMap[user].keys())
    months = set()
    for date in dates:
        if date > datetime.utcfromtimestamp(0):
            months.add(date.month)
    return len(months)

def filterBasedOnActiveMonths(users, op, numMonths):
    """
    Filter users based on how long where they active
    """
    if op == Ops.Eq:
        return [user for user in users if getNumMonthsActive(user) == numMonths]
    elif op == Ops.Lt:
        return [user for user in users if getNumMonthsActive(user) < numMonths]
    elif op == Ops.LtE:
        return [user for user in users if getNumMonthsActive(user) <= numMonths]
    elif op == Ops.Gt:
        return [user for user in users if getNumMonthsActive(user) > numMonths]
    elif op == Ops.GtE:
        return [user for user in users if getNumMonthsActive(user) >= numMonths]
    else:
        raise Exception("Unknown op")

def tweetFrequency(users):
    """
    Some statistics about tweet frequency
    """
    freqData = dict()
    for user in users:
        tweets = userToTweetsMap.get(user, None)
        if tweets is None:
            continue
        else:
            tweetDates = map(strToDate, tweets.keys())
            firstDate = min(tweetDates)
            lastDate = max(tweetDates)
            numDaysTweeted = len(tweetDates)
            dateRangeActive = (lastDate - firstDate).days + 1
            numTweets = 0
            for t in tweets.values():
                numTweets += len(t)
            freqData[user] = (dateRangeActive, numDaysTweeted, numTweets)
    return freqData


def readRawData():
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

def readParsedData():
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
        readRawData()
    readParsedData()
    print len(twitterUsers)
    allUsers = twitterUsers.keys()

    start_end_users = dict()
    for end in range(9,13):
        endUsers = filterUsersByEndMonth(allUsers, end, 2009)
        for start in range(9, end+1):
            if start not in start_end_users.keys():
                start_end_users[start] = dict()
            print start, end
            duration = end - start + 1
            start_end_users[start][end] = filterBasedOnActiveMonths(
                    endUsers,
                    Ops.Eq,
                    duration)

    for start, v in start_end_users.iteritems():
        for end, users in v.iteritems():
            print start, end
            freqData = tweetFrequency(users)
            avgTweetPerDay = 0
            avgTweetPerDayOnTweetDays = 0
            for (rng_active, num_tweet_days, num_tweets) in freqData.values():
                avgTweetPerDay += 1.0*num_tweets/rng_active
                avgTweetPerDayOnTweetDays += 1.0*num_tweets/num_tweet_days
            print avgTweetPerDay/len(users), avgTweetPerDayOnTweetDays/len(users), len(users)

    randomUsers = sampleUsers(10000)
    randomUsersFreq = tweetFrequency(randomUsers)
    avgTweetPerDay = 0
    avgTweetPerDayOnTweetDays = 0
    for (rng_active, num_tweet_days, num_tweets) in freqData.values():
        if (num_tweet_days > 0):
            avgTweetPerDay += 1.0*num_tweets/rng_active
            avgTweetPerDayOnTweetDays += 1.0*num_tweets/num_tweet_days
    print avgTweetPerDay/len(randomUsers), avgTweetPerDayOnTweetDays/len(randomUsers), len(randomUsers)
