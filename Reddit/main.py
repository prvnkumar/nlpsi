import os
import json
import re
from collections import defaultdict
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from math import floor
import nltk
from nltk.tokenize import WordPunctTokenizer
from nltk.corpus import stopwords
from nltk import word_tokenize
import numpy
import pickle
import shutil
import sys
import string
from vaderSentiment.vaderSentiment import sentiment as vaderSentiment
__author__ = 'Eva Sharma and Praveen Kumar'

from lm import *

#Directory Paths
ROOT_DIR = (os.path.dirname(__file__)) # This is your Project Root
RAW_DATA_PATH = os.path.join(ROOT_DIR,"RawData") #Folder to keep raw jsonlist files
PROC_DATA_PATH = os.path.join(ROOT_DIR,"ProcessedData")
REGUSERLIST = os.path.join(PROC_DATA_PATH,"reg_user_list.txt")
LM_PATH = os.path.join(PROC_DATA_PATH,"lm.txt")
SENT_ANALYSIS = False

word_punct_tokenizer = WordPunctTokenizer()
stop = stopwords.words('english')

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

def storeDataForUser(comment, lm):
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
    lm.update(commentText)

    with open(os.path.join(PROC_DATA_PATH,str(comment["author"])), 'a+') as outfile:
        json.dump(comment, outfile)
        outfile.write("\n")

def storeComment(regularUsers, comment, lm):
    if len(comment['children']) > 0:
        for child in comment['children']:
            storeComment(regularUsers, child, lm)
    if (comment["author"] in regularUsers):
        if (comment.get("selftext") != "None" and \
                comment.get("selftext")!= '') or \
                (comment.get("body") != "None" and comment.get("body")!= ''):
            storeDataForUser(comment, lm)

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

def findAndStoreRegularUsers():
    regularUsers = []
    lm = LM()
    if os.path.isfile(REGUSERLIST) and os.path.isfile(LM_PATH):
        print 'Reading REGUSERLIST'
        with open(REGUSERLIST, 'r') as f:
            regularUsers = [name.rstrip('\n') for name in f.readlines()]
        print 'Reading LM_PATH'
        with open(LM_PATH, 'r') as f:
            lm = pickle.load(f)
        return regularUsers, lm

    shutil.rmtree(PROC_DATA_PATH, ignore_errors=True)
    os.mkdir(PROC_DATA_PATH)
    for fileName in os.listdir(RAW_DATA_PATH):
        print 'Reading :', fileName
        if fileName.endswith(".txt") or fileName.endswith(".jsonlist"):
            # load the json List from every fileName
            jsonList = json.load(
                    open(os.path.join(RAW_DATA_PATH, fileName)),
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
                if numComments > 30:
                    regularUsers.append(author)
                    numCommentsByRegUsers += numComments

            print "Total number of users:", len(numCommentsPerAuthor)
            print "Total number of comments by all users:", totalNumComments
            print "Number of users who had comments > 50:", len(regularUsers)
            print "Total number of comments by such users:", numCommentsByRegUsers
            print "------------------"

    print "Storing comments for regular users"
    for fileName in os.listdir(RAW_DATA_PATH):
        if fileName.endswith(".txt") or fileName.endswith(".jsonlist"):
            jsonList = json.load(
                   open(os.path.join(RAW_DATA_PATH, fileName)),
                   cls=ConcatJSONDecoder)
            for comment in jsonList:
                storeComment(regularUsers, comment, lm)
    print "storing done"
    with open(REGUSERLIST, 'w') as f:
        f.write('\n'.join(regularUsers))
    with open(LM_PATH, 'w') as f:
        pickle.dump(lm, f)
    return regularUsers, lm

def isActiveAfterSOFFor(commentDates, sofTime, n):
    sofPlusxMonths = sofTime + relativedelta(months=n)

    isActive = False
    count = 0
    for timestamp in commentDates:
        time = datetime.utcfromtimestamp(timestamp)
        if time > sofTime and time < sofPlusxMonths:
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


def findUsersWhoQuit(regularUsers, lm):
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
    commentTextsForUser = dict()
    commentsForUser = dict()
    print 'Find users who quit'
    for user in regularUsers:
        print '.',
        comments = json.load(
                open(os.path.join(PROC_DATA_PATH, user)),
                cls=ConcatJSONDecoder)

        nres = 0
        ncom = 0
        commentDatesList = []
        responseSentiment[user] = []
        commentSentiment[user] = []
        commentWithNoReponses[user] = 0
        commentsForUser[user] = []
        for comment in comments:
            commentsForUser[user].append(comment)
            commentDatesList.append(float(comment['created_utc']))
            numSubComments = lenSubComments(comment)
            if numSubComments == 0:
                commentWithNoReponses[user]+=1
            nres += numSubComments
            ncom += 1
            if SENT_ANALYSIS:
                responseSentiment[user].append(getSentimentResponse(comment))
                commentSentiment[user].append(vaderSentiment(comment.get('selftext').encode('utf-8')))
        numAvgResponses[user] = nres/ncom
        commentWithNoReponses[user] = floor(commentWithNoReponses[user])/floor(ncom)
        commentDates[user] = commentDatesList
        lastCommentDates.append(max(commentDatesList))

    sofTimets = median(lastCommentDates)
    sofTime = datetime.utcfromtimestamp(sofTimets)
    print sofTime

    for user in regularUsers:
        if isActiveAfterSOFFor(commentDates[user], sofTime, 3):
            activeUsers.append(user)
        else:
            quitters.append(user)
        commentTextsForUser[user] = [comment["selftext"] for comment in commentsForUser[user] if float(comment['created_utc']) <= sofTimets]
    quitters = quitters[:len(activeUsers)]
    print len(quitters)
    print len(activeUsers)
    print "Quitters"

    print "For Quitter Num No response : " , numpy.average([commentWithNoReponses[user] for user in quitters])
    print "For Active Num No response : " , numpy.average([commentWithNoReponses[user] for user in activeUsers])

    if SENT_ANALYSIS:
        nc = 0
        pos_quit  = 0
        neg_quit  = 0
        neu_quit  = 0
        for user in quitters:
            for csent in commentSentiment[user]:
                pos_quit += csent['pos']
                neg_quit += csent['neg']
                neu_quit += csent['neu']
                nc += 1
        pos_quit = float(pos_quit)/nc
        neg_quit = float(neg_quit)/nc
        neu_quit = float(neu_quit)/nc

        print 'Quitter\'s comment sentiment pos neg neu:', pos_quit, neg_quit, neu_quit
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
        neu_act  = 0
        for user in activeUsers:
            for csent in commentSentiment[user]:
                pos_act += csent['pos']
                neg_act += csent['neg']
                neu_act += csent['neu']
                nc += 1
        pos_act = float(pos_act)/nc
        neg_act = float(neg_act)/nc
        neu_act = float(neu_act)/nc

        print 'Active user\'s comment sentiment pos neg neu:', pos_act, neg_act, neu_act

        for user in activeUsers:
            avg_sentiment += average(responseSentiment[user])
            avg_sentiment_correlation += measure_correlation(
                    commentSentiment[user],
                    responseSentiment[user])
        print 'Active users: Avg sentiment of responses:', avg_sentiment/len(activeUsers)
        print 'Active users: comment result correlation:', avg_sentiment_correlation/len(activeUsers)

    nr = 0
    for user in quitters:
        nr += numAvgResponses[user]
    print 'Quitters num avg # response:', float(nr)/len(quitters)
    nr = 0
    for user in activeUsers:
        nr += numAvgResponses[user]
    print 'Active users avg # response:', float(nr)/len(activeUsers)

    avgNumComQ = 0
    avgNumComA = 0
    avgNumCom = 0
    for user in quitters:
        avgNumCom += len(commentTextsForUser[user])
        avgNumComQ += len(commentTextsForUser[user])
    for user in activeUsers:
        avgNumCom += len(commentTextsForUser[user])
        avgNumComA += len(commentTextsForUser[user])

    print 'Num of comments: ', avgNumCom, avgNumComQ, avgNumComA
    print 'Num users: ', len(quitters)+len(activeUsers), len(quitters),len(activeUsers)
    avgNumCom = avgNumCom/(len(quitters) + len(activeUsers))
    avgNumComQ = avgNumComQ/len(quitters)
    avgNumComA = avgNumComA/len(activeUsers)
    print 'Avg num of comments: ', avgNumCom, avgNumComQ, avgNumComA
    minAvgNumCom = min(avgNumComQ, avgNumComA)

    print "Language modeling"
    uniqueWords = dict()
    numCommentsConsidered = dict()
    totalWordsConsidered = dict()
    uniqueWordsFraction = dict()
    jsdiv = dict()
    nComQ = 0
    nWQ = 0
    nComA = 0
    nWA = 0
    Q = []
    A = []
    for user in quitters:
        if user not in commentTextsForUser.keys():
            continue
        num_uw, frac_uw, num_tw, num_com, js_div = createUNiGramModelFromUserComments(user,commentTextsForUser[user][:minAvgNumCom], lm)
        uniqueWords[user] = num_uw
        totalWordsConsidered[user] = num_tw
        numCommentsConsidered[user] = num_com
        uniqueWordsFraction[user] = frac_uw
        jsdiv[user] = js_div
        nComQ += num_com
        nWQ += num_tw
        Q.append(user)

    for user in activeUsers:
        if user not in commentTextsForUser.keys():
            continue
        num_uw, frac_uw, num_tw, num_com, js_div = createUNiGramModelFromUserComments(user,commentTextsForUser[user][:minAvgNumCom], lm)
        uniqueWords[user] = num_uw
        totalWordsConsidered[user] = num_tw
        numCommentsConsidered[user] = num_com
        uniqueWordsFraction[user] = frac_uw
        jsdiv[user] = js_div
        nComA += num_com
        nWA += num_tw
        A.append(user)
        if nComQ < nComA:
            break
    print nComQ, nComA, nWQ, nWA

    print "Q Avg # unique words :\t" , numpy.average([uniqueWords[user] for user in Q])
    print "A Avg # unique words :\t" , numpy.average([uniqueWords[user] for user in A])

    print "Q # total words :\t" , numpy.average([totalWordsConsidered[user] for user in Q])
    print "A # total words :\t" , numpy.average([totalWordsConsidered[user] for user in A])

    print "Q # total comments :\t" , numpy.average([numCommentsConsidered[user] for user in Q])
    print "A # total comments :\t" , numpy.average([numCommentsConsidered[user] for user in A])

    print "Q Fraction of unique words :\t" , numpy.average([uniqueWordsFraction[user] for user in Q])
    print "A Fraction of unique words :\t" , numpy.average([uniqueWordsFraction[user] for user in A])

    print "Q JS Divergence :\t" , numpy.average([jsdiv[user] for user in Q])
    print "A JS Divergence :\t" , numpy.average([jsdiv[user] for user in A])



def createUNiGramModelFromUserComments(user,comments, lm):
    commentTokens = list()
    js_div = lm.jsdivergence(' '.join(comments))
    for comment in comments:
        text = nltk.Text(word_punct_tokenizer.tokenize(comment.lower()))[:40]
        tempList = [i for i in text if i not in stop]
        commentTokens.extend(tempList)
    return len(set(commentTokens)), float(len(set(commentTokens)))/(len(commentTokens)+1), len(commentTokens), len(comments), js_div

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
    regularUsers, lm = findAndStoreRegularUsers()
    print len(regularUsers)
    findUsersWhoQuit(regularUsers, lm)
