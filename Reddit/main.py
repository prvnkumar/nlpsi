import os
import json
import re
from collections import defaultdict
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import dataset
from multiprocessing import Pool
import numpy
import shutil
import sys
from vaderSentiment.vaderSentiment import sentiment as vaderSentiment

__author__ = 'Eva Sharma and Praveen Kumar'

# Global variables
regularUsers = []

#Directory Paths
projectRootDir = (os.path.dirname(__file__)) # This is your Project Root
rawDataPath = os.path.join(projectRootDir,"RawData") #Folder to keep raw jsonlist files
processedDataPath = os.path.join(projectRootDir,"ProcessedData")

shutil.rmtree(processedDataPath, ignore_errors=True)
os.mkdir(processedDataPath)


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
    comment['selftext'] = commentText
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

def lenSubComments(comment):
    n = len(comment['children'])
    for c in comment['children']:
        n += lenSubComments(c)
    return n

def worker(response):
    selftext = response.get('selftext', None)
    if selftext is not None:
        return vaderSentiment(selftext.encode('utf-8'))['compound']
    return 0


def getSentimentResponse(comment):
    """
    For each comment in comments, return the sentiment of direct responses
    """
    sentimentPerComment = []
    for response in comment['children']:
        sentimentPerComment.append(worker(response))
    return sentimentPerComment


def findUsersWhoQuit():
    """
    Get users who quit
    """
    activeUsers = []
    quitters = []
    commentDates = dict()
    lastCommentDates = []
    numAvgResponses = dict()
    responseSentiment = dict()
    commentSentiment = dict()
    print 'Find users who quit'
    for user in regularUsers:
        comments = json.load(
                open(os.path.join(processedDataPath, user)),
                cls=ConcatJSONDecoder)
        nres = 0
        ncom = 0
        commentDatesList = []
        responseSentiment[user] = []
        commentSentiment[user] = []
        for comment in comments:
            commentDatesList.append(float(comment['created_utc']))
            nres += lenSubComments(comment)
            ncom += 1
            responseSentiment[user].append(getSentimentResponse(comment))
            commentSentiment[user].append(vaderSentiment(comment.get('selftext').encode('utf-8'))['compound'])
        numAvgResponses[user] = nres/ncom
        commentDates[user] = commentDatesList
        lastCommentDates.append(max(commentDatesList))

    sofTime = datetime.utcfromtimestamp(median(lastCommentDates))
    print sofTime

    for user in regularUsers:
        if isActiveAfterSOFFor(commentDates[user], sofTime, 6):
            activeUsers.append(user)
        else:
            quitters.append(user)

    print len(quitters)
    print len(activeUsers)
    print "Quitters"
    avg_sentiment = 0
    avg_sentiment_correlation = 0
    for user in quitters:
        avg_sentiment += average(responseSentiment[user])
        avg_sentiment_correlation += measure_correlation(
                commentSentiment[user],
                responseSentiment[user])
    print avg_sentiment/len(quitters)
    print avg_sentiment_correlation/len(quitters)

    avg_sentiment = 0
    avg_sentiment_correlation = 0
    print "Active"
    for user in activeUsers:
        avg_sentiment += average(responseSentiment[user])
        avg_sentiment_correlation += measure_correlation(
                commentSentiment[user],
                responseSentiment[user])
    print avg_sentiment/len(activeUsers)
    print avg_sentiment_correlation/len(activeUsers)

    nc = 0
    for user in quitters:
        nc += numAvgResponses[user]
    print 'Quitters num avg # response:', float(nc)/len(quitters)
    nc = 0
    for user in activeUsers:
        nc += numAvgResponses[user]
    print 'Active users avg # response:', float(nc)/len(activeUsers)


def measure_correlation(comment_sent, response_sent_list):
    avg_response_sent = []
    for lst in response_sent_list:
        avg_response_sent.append(numpy.average(lst))
    return numpy.corrcoef(comment_sent, avg_response_sent)[0, 1]

def average(ll):
    n = 0
    sum = 0
    for l in ll:
        n += len(l)
        for x in l:
            sum += x
    if n==0:
        return 0
    return sum/n


if __name__ == "__main__":
    main()
    findUsersWhoQuit()
