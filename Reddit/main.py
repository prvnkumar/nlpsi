__author__ = 'evsharma and Praveen Kumar'

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

# Global variables
regularUsers = []

#Directory Paths
projectRootDir = (os.path.dirname(__file__)) # This is your Project Root
rawDataPath = os.path.join(projectRootDir,"RawData") #Folder to keep raw jsonlist files
processedDataPath = os.path.join(projectRootDir,"ProcessedData")

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
    """
    Store per user data in file
    """
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

def storeComment(comment):
    if len(comment['children']) > 0:
        for child in comment['children']:
            storeComment(child)
    if (comment["author"] in regularUsers):
        if (comment.get("selftext") != "None" and \
                comment.get("selftext")!= '') or \
                (comment.get("body") != "None" and comment.get("body")!= ''):
            storeDataForUser(comment)

def calcCommentsPerAuthor(comment, numCommentsPerAuthor):
    """
    Calculate number of comments per author
    """
    if len(comment['children']) > 0:
        for child in comment['children']:
            calcCommentsPerAuthor(child, numCommentsPerAuthor)
    if (comment["author"] != "[deleted]" and \
            comment["author"] != None and \
            comment["author"] != '') and \
            ((comment.get("selftext") != "None" and comment.get("selftext")!= '') \
              or (comment.get("body") != "None" and comment.get("body")!= '')) :
        author = comment['author']
        n = numCommentsPerAuthor.get(author, 0)
        numCommentsPerAuthor[author] = n+1

def main():
    global regularUsers
    for fileName in os.listdir(rawDataPath):
        print fileName
        if fileName.endswith(".txt") or fileName.endswith(".jsonlist"):
            # load the json List from every fileName
            jsonList = json.load(
                    open(os.path.join(rawDataPath, fileName)),
                    cls=ConcatJSONDecoder)
            print "file loaded."
            numCommentsPerAuthor = dict()
            for comment in jsonList:
                calcCommentsPerAuthor(comment, numCommentsPerAuthor)

            numCommentsByRegUsers = 0
            totalNumComments = 0
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

    print "Storing comments for regular users"
    for fileName in os.listdir(rawDataPath):
        if fileName.endswith(".txt") or fileName.endswith(".jsonlist"):
            jsonList = json.load(
                    open(os.path.join(rawDataPath, fileName)),
                    cls=ConcatJSONDecoder)
            for comment in jsonList:
                storeComment(comment)

def isActiveAfterSOFFor(commentDates, sofTime, nMonths):
    sofPlusSixMonths = sofTime + relativedelta(months=+6)

    isActive = False
    count = 0
    for timestamp in commentDates:
        time = datetime.utcfromtimestamp(timestamp)
        if time > sofTime and time < sofPlusSixMonths:
            count += 1
            if count >= 2:
                isActive = True
                break
    return isActive

def median(s):
    i = len(s)
    l = sorted(s)
    if not i%2:
        return (l[(i/2)-1]+l[i/2])/2.0
    return l[i/2]

def findUsersWhoQuit():
    """
    Get users who quit
    """
    activeUsers = set()
    quitters = set()
    commentDates = dict()
    lastCommentDates = []
    for user in regularUsers:
        comments = json.load(
                open(os.path.join(processedDataPath, user)),
                cls=ConcatJSONDecoder)
        commentDates[user] = [float(comment['created_utc'])
                for comment in comments]
        lastCommentDates.append(max(commentDates[user]))

    sofTime = datetime.utcfromtimestamp(median(lastCommentDates))
    print sofTime

    for user in regularUsers:
        if isActiveAfterSOFFor(commentDates[user], sofTime, 6):
            activeUsers.add(user)
        else:
            quitters.add(user)

    print len(quitters)
    print len(activeUsers)


if __name__ == "__main__":
    main()
    findUsersWhoQuit()
