import os
import json
import re
from collections import defaultdict
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import dataset
from multiprocessing import Pool
from math import floor
import numpy
import shutil
import sys
from vaderSentiment.vaderSentiment import sentiment as vaderSentiment
import nltk
from nltk.tokenize import WordPunctTokenizer
from nltk.corpus import stopwords
import string
from nltk import word_tokenize
__author__ = 'Eva Sharma and Praveen Kumar'

# Global variables
regularUsers = []
REPROCESS=False

#Directory Paths
projectRootDir = (os.path.dirname(__file__)) # This is your Project Root
rawDataPath = os.path.join(projectRootDir,"RawData") #Folder to keep raw jsonlist files
processedDataPath = os.path.join(projectRootDir,"ProcessedData")

if REPROCESS:
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
                if numComments > 50:
                    regularUsers.append(author)
                    numCommentsByRegUsers += numComments

            print "Total number of users:", len(numCommentsPerAuthor)
            print "Total number of comments by all users:", totalNumComments
            print "Number of users who had comments > 50:", len(regularUsers)
            print "Total number of comments by such users:", numCommentsByRegUsers
            print "------------------"

    if REPROCESS:
        print "Storing comments for regular users"
        for fileName in os.listdir(rawDataPath):
            if fileName.endswith(".txt") or fileName.endswith(".jsonlist"):
                jsonList = json.load(
                       open(os.path.join(rawDataPath, fileName)),
                       cls=ConcatJSONDecoder)
                for comment in jsonList:
                    storeComment(comment)
        print "soring done"

def isActiveAfterSOFFor(commentDates, sofTime):
    sofPlusSixMonths = sofTime + relativedelta(months=+12)

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
    commentWithNoReponses = dict()
    commentsForUsers = dict()
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
        commentWithNoReponses[user] = 0
        commentsForUsers[user] = list()
        for comment in comments:
            commentsForUsers[user].append(comment["selftext"])
            commentDatesList.append(float(comment['created_utc']))
            numSubComments = lenSubComments(comment)
            if numSubComments == 0:
                commentWithNoReponses[user]+=1
            nres += numSubComments
            ncom += 1
            responseSentiment[user].append(getSentimentResponse(comment))
            commentSentiment[user].append(vaderSentiment(comment.get('selftext').encode('utf-8')))
        numAvgResponses[user] = nres/ncom
        commentWithNoReponses[user] = floor(commentWithNoReponses[user])/floor(ncom)
        commentDates[user] = commentDatesList
        lastCommentDates.append(max(commentDatesList))

    sofTime = datetime.utcfromtimestamp(median(lastCommentDates))
    print sofTime

    for user in regularUsers:
        if isActiveAfterSOFFor(commentDates[user], sofTime):
            activeUsers.append(user)
        else:
            quitters.append(user)

    print len(quitters)
    print len(activeUsers)
    print "Quitters"

    print "For Quitter Num No response : " , numpy.average([commentWithNoReponses[user] for user in quitters])
    print "For Active Num No response : " , numpy.average([commentWithNoReponses[user] for user in activeUsers])


    nc = 0
    pos_quit  = 0
    neg_quit  = 0
    ntr_quit  = 0
    for user in quitters:
        for csent in commentSentiment[user]:
            pos_quit += csent['positive']
            neg_quit += csent['negative']
            ntr_quit += csent['neutral']
            nc += 1
    pos_quit = float(pos_quit)/nc
    neg_quit = float(neg_quit)/nc
    ntr_quit = float(ntr_quit)/nc

    print 'Quitter\'s comment sentiment pos neg ntr:', pos_quit, neg_quit, ntr_quit
    avg_sentiment = 0
    avg_sentiment_correlation = 0
    for user in quitters:
        avg_sentiment += average(responseSentiment[user])
        avg_sentiment_correlation += measure_correlation(
                commentSentiment[user],
                responseSentiment[user])
    print 'Quitters: Avg sentiment of responses:', avg_sentiment/len(quitters)
    print 'Quitters: comment result correlation:', avg_sentiment_correlation/len(quitters)

    avg_sentiment = 0
    avg_sentiment_correlation = 0
    print "Active"

    nc = 0
    pos_act  = 0
    neg_act  = 0
    ntr_act  = 0
    for user in activeUsers:
        for csent in commentSentiment[user]:
            pos_act += csent['positive']
            neg_act += csent['negative']
            ntr_act += csent['neutral']
            nc += 1
    pos_act = float(pos_act)/nc
    neg_act = float(neg_act)/nc
    ntr_act = float(ntr_act)/nc

    print 'Active user\'s comment sentiment pos neg ntr:', pos_act, neg_act, ntr_act
 
    for user in activeUsers:
        avg_sentiment += average(responseSentiment[user])
        avg_sentiment_correlation += measure_correlation(
                commentSentiment[user],
                responseSentiment[user])
    print 'Active users: Avg sentiment of responses:', avg_sentiment/len(activeUsers)
    print 'Active users: comment result correlation:', avg_sentiment_correlation/len(activeUsers)

    nc = 0
    for user in quitters:
        nc += numAvgResponses[user]
    print 'Quitters num avg # response:', float(nc)/len(quitters)
    nc = 0
    for user in activeUsers:
        nc += numAvgResponses[user]
    print 'Active users avg # response:', float(nc)/len(activeUsers)

    print "NLTK work"
    for user in commentsForUsers.keys():
        createUNiGramModelFromUserComments(user,commentsForUsers[user])

    print "For Quitter Num of unique words used : " , numpy.average([uniqueWordsForUser[user] for user in quitters])
    print "For Active Num of unique words used : " , numpy.average([uniqueWordsForUser[user] for user in activeUsers])





uniqueWordsForUser = dict()
word_punct_tokenizer = WordPunctTokenizer()
stop = stopwords.words('english')
def createUNiGramModelFromUserComments(user,comments):

    commentTokens = list()

    for comment in comments:
        text = nltk.Text(word_punct_tokenizer.tokenize(comment.lower()))
        tempList = [i for i in text if i not in stop]
        commentTokens.extend(tempList)

    uniqueWordsForUser[user] = len(list(set(commentTokens)))




def measure_correlation(comment_sent, response_sent_list):
    avg_response_sent = []
    compound_comment_sent = [x['compound'] for x in comment_sent]
    for lst in response_sent_list:
        avg_response_sent.append(numpy.average(lst) if len(lst) > 0 else 0)
    return numpy.cov(compound_comment_sent, avg_response_sent)[0, 1]

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
