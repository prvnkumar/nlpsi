__author__ = 'evsharma'

import os
import json
import re
from collections import defaultdict
import calendar
from datetime import datetime, timedelta
import dataset
import sys
import numpy
from dateutil.relativedelta import relativedelta


#Directory Paths
projectRootDir = (os.path.dirname(__file__)) # This is your Project Root
rawDataPath = os.path.join(projectRootDir,"RawData") #Folder to keep raw jsonlist files
processedDataPath = os.path.join(projectRootDir,"ProcessedData")
#create DB
# connecting to a SQLite database
# db = dataset.connect('sqlite:///mydatabase.db')

#list of users
users = set()

###
### Read/Load json
###
FLAGS = re.VERBOSE | re.MULTILINE | re.DOTALL
WHITESPACE = re.compile(r'[ \t\n\r]*', FLAGS)

class ConcatJSONDecoder(json.JSONDecoder):
    def decode(self, s, _w=WHITESPACE.match):
        s_len = len(s)

        end = 0
        while end != s_len:
            obj, end = self.raw_decode(s, idx=_w(s, end).end())
            end = _w(s, end).end()
            yield obj

def storeDataForUser(comment):
    # get a reference to the table 'user'
    # table = db[(comment["author"])]

    if comment.get("selftext") !=  None :
        commentText = comment["selftext"]
    elif comment.get("body") != None:
        commentText = comment["body"]
    else:
        commentText = None
    comment['selfText'] = commentText
    comment.pop('body', None)

    with open(os.path.join(processedDataPath,str(comment["author"])), 'a+') as outfile:
        json.dump(comment, outfile)
    # Insert a new record.
    # table.insert(tableDict)

path = rawDataPath
print(path)

def recursionForStoringData(comment):
    if len(comment['children']) > 0:
        for child in comment['children']:
            recursionForStoringData(child)
    else:
        if (comment["author"] in totalUsers):
            if (comment.get("selftext") != "None" and comment.get("selftext")!= '') or (comment.get("body") != "None" and comment.get("body")!= ''):
                storeDataForUser(comment)


def populateUserList(comment, numCommentsPerAuthor):
    if len(comment['children']) > 0:
        for child in comment['children']:
            populateUserList(child, numCommentsPerAuthor)
    if (comment["author"] != "[deleted]" and comment["author"] != None and comment["author"] != '') and ((comment.get("selftext") != "None" and comment.get("selftext")!= '') or (comment.get("body") != "None" and comment.get("body")!= '')) :
        author = comment['author']
        n = numCommentsPerAuthor.get(author, 0)
        numCommentsPerAuthor[author] = n+1

totalUsers = set()

def main():
    for file in os.listdir(path):
        print file
        if file.endswith(".txt") or file.endswith(".jsonlist"):
            # load the json List from every file
            jsonList = json.load(open(os.path.join(path,file)),cls=ConcatJSONDecoder)
            print "File loaded."
            numCommentsPerAuthor = dict()
            for comment in jsonList:
                populateUserList(comment, numCommentsPerAuthor)

            numCommentsByRegUsers = 0
            totalNumComments = 0
            regularUsers = []
            for author in numCommentsPerAuthor.keys():
                numComments = numCommentsPerAuthor[author]
                totalNumComments += numComments
                if numComments > 20:
                    regularUsers.append(author)
                    numCommentsByRegUsers += numComments

            print "Total number of users:", len(numCommentsPerAuthor)
            print "Total number of comments by all users:", totalNumComments
            print "Number of users who had comments > 20:", len(regularUsers)
            print "Total number of comments by such users:", numCommentsByRegUsers
            print "------------------"

    print "Now populating data"
    count = 0
    for file in os.listdir(path):
        if file.endswith(".txt") or file.endswith(".jsonlist"):
            # load the json List from every file
            jsonList = json.load(open(os.path.join(path,file)),cls=ConcatJSONDecoder)
            # From the JsonList populate the DB for every user
            for comment in jsonList:
                recursionForStoringData(comment)
    print count

def checkIfInactiveAfterSOFForSixMonths(user,sofTime):
    sofPlusSixMonths = sofTime + relativedelta(months=+6)
    timeArray = list()
    for data in user:
        timeArray.append(datetime.utcfromtimestamp(data["created_utc"]).date())
    timeArray = list(set(timeArray))

    isActive = False
    count = 0
    for time in timeArray:
        if time > sofTime and time < sofPlusSixMonths:
            count+=1

    if count >=2 :
        isActive = True
    else:
        isActive = False

    return isActive

sof = set()

def findSOF(user):

    timeArray = list()
    for data in user:
        timeArray.append(datetime.utcfromtimestamp(data["created_utc"]).date())
    timeArray = list(set(timeArray))
    timeArray.sort(reverse=True)
    sof.add(timeArray[0])

usersWhoQuit = set()
activeUsers = set()
quitters = set()

def median(s):
    i = len(s)
    if not i%2:
        return (s[(i/2)-1]+s[i/2])/2.0
    return s[i/2]

def findUsersWhoQuit():


    for file in os.listdir(processedDataPath):
        jsonList = json.load(open(os.path.join(processedDataPath,file)),cls=ConcatJSONDecoder)
        findSOF(jsonList)

    sofTime = median(list(sof))
    print sofTime

    for file in os.listdir(processedDataPath):
        jsonList = json.load(open(os.path.join(processedDataPath,file)),cls=ConcatJSONDecoder)
        if checkIfInactiveAfterSOFForSixMonths(jsonList,sofTime) == True:
            activeUsers.add(file)
        else:
            quitters.add(file)





    print str(len(quitters))
    print str(len(activeUsers))


if __name__ == "__main__":
    main()
    #findUsersWhoQuit()
