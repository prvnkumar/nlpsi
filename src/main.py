__author__ = 'evsharma'

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
import json
import os

BASE_DIR = os.path.dirname(__file__)
trainingSetTweetsFilePath = os.path.join(BASE_DIR,"twitter/training_set_tweets.txt")
trainingSetUsersFilePath = os.path.join(BASE_DIR,"twitter/training_set_users.txt")
testSetTweetsFilePath = os.path.join(BASE_DIR,"twitter/test_set_tweets.txt")
testSetUsersFilePath = os.path.join(BASE_DIR,"twitter/twitter/test_set_users.txt")


userToTweetsMap = dict()
tweets = dict()
twitterUsers = dict()

def populateTweets(tweetId,tweet):
    #populate the dictionary tweets with key as tweet id and value as the tweet
    tweets[tweetId] = tweet

def populateTwitterUsers(userId,location):
    #populate the dictionary twitterUsers with key as twitter user id and value as array having index 0 as the
    # state and index 1 as the city
    twitterUsers[userId] = location.split(",")
    if(len(twitterUsers[userId]) > 1):
        twitterUsers[userId][1] = twitterUsers[userId][1].strip()


def populateUserToTweetsMap(userId,date,tweetId):
    """a dictionary :
    key : twitter user id
    value:  date :twitter id
    """
    if not userId in userToTweetsMap:
        userToTweetsMap[userId] = dict()
        userToTweetsMap[userId][date] = tweetId
    else:
        userToTweetsMap[userId][date] = tweetId


def readUsersFile(file):
    f = open(file,'r')
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

def getUsersGoingInactiveAfterNov(user):
    if user in userToTweetsMap.keys():
        dates = list(userToTweetsMap[user].keys())
        dates.sort()
        #print(dates)
        if isActiveFromDec2009(dates) == False:
            if isActiveinNov2009(dates) == True:
                numMonthsActive = getNumMonthsActive(dates)
                print("User : ",user,"Months Active: ",numMonthsActive+1)





def isActiveFromDec2009(dates):
    isActive = False
    for date in dates:
        try:
            d = datetime.strptime(date, '%Y-%m-%d')
            if(d.year == 2010 or (d.year == 2009 and d.month >11)):
                isActive = True
        except ValueError:
            continue
    return isActive

def isActiveinNov2009(dates):
    isActive = False
    for date in dates:
        try:
            d = datetime.strptime(date, '%Y-%m-%d')
            if(d.year == 2009 and d.month ==11):
                isActive = True
        except ValueError:
            continue
    return isActive

def getNumMonthsActive(dates):
    months = list()
    for date in dates:
        try:
            d = datetime.strptime(date, '%Y-%m-%d')
            if(d.month<11 and d.year < 2010):
                months.append(d.month)
        except ValueError:
            continue
    return len(list(set(months)))


if __name__ == '__main__':

    print("Reading ",trainingSetUsersFilePath)
    readUsersFile(trainingSetUsersFilePath)
    print("\nReading ",trainingSetTweetsFilePath)
    readTweetsFile(trainingSetTweetsFilePath)
    print("\nWriting To files")
    writeToFile(os.path.join(BASE_DIR,"Dict/twitterUsers.txt"),twitterUsers)
    writeToFile(os.path.join(BASE_DIR,"Dict/tweets.txt"),tweets)
    writeToFile(os.path.join(BASE_DIR,"Dict/userToTweetsMap.txt"),userToTweetsMap)
    print("\nPopulating twitterUsers")
    twitterUsers = json.load(open(os.path.join(BASE_DIR,"Dict/twitterUsers.txt")))
    print("\nPopulating tweets")
    tweets = json.load(open(os.path.join(BASE_DIR,"Dict/tweets.txt")))
    print("\nPopulating userToTweetsMap")
    userToTweetsMap = json.load(open(os.path.join(BASE_DIR,"Dict/userToTweetsMap.txt")))


    for user in twitterUsers.keys():
        getUsersGoingInactiveAfterNov(user)

