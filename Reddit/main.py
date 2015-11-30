import os
import json
import re
from collections import defaultdict, OrderedDict
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from math import floor
import multiprocessing as mp
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
from mrc import MRC

l = mp.Lock()
manager = mp.Manager()
global_data = manager.Namespace()
global_data.total_started = 0
global_data.total_running = 0

ROOT_DIR = (os.path.dirname(__file__)) # This is your Project Root
RAW_DATA_PATH = os.path.join(ROOT_DIR,"RawData") #Folder to keep raw jsonlist files

class ConcatJSONDecoder(json.JSONDecoder):

	###
	### Read/Load json
	###
	FLAGS = re.VERBOSE | re.MULTILINE | re.DOTALL
	WHITESPACE = re.compile(r'[ \t\n\r]*', FLAGS)

	def decode(self, s, _w=WHITESPACE.match):
		s_len = len(s)

		end = 0
		while end != s_len:
			obj, end = self.raw_decode(s, idx=_w(s, end).end())
			end = _w(s, end).end()
			yield obj


class Model:
    #Directory Paths
    RAW_DATA_PATH = os.path.join(ROOT_DIR,"RawData") #Folder to keep raw jsonlist files
    PROC_DATA_PATH = os.path.join(ROOT_DIR,"ProcessedData")
    REGUSERLIST = os.path.join(PROC_DATA_PATH,"reg_user_list.txt")
    LM_PATH = os.path.join(PROC_DATA_PATH,"lm.txt")
    SENT_ANALYSIS = False
    STATS_FILE = 'stats.txt'

    word_punct_tokenizer = WordPunctTokenizer()
    stop = stopwords.words('english')
    lang_model = None
    subreddit = None

    mem_buffer = {}

    def __init__(self, subreddit):
        self.subreddit = subreddit
        self.PROC_DATA_PATH = os.path.join(ROOT_DIR,"ProcessedData"+subreddit)
        self.REGUSERLIST = os.path.join(self.PROC_DATA_PATH,"reg_user_list.txt")
        self.LM_PATH = os.path.join(self.PROC_DATA_PATH,"lm.txt")
        
    def bufferAdd(self, path, comment):
        if path not in self.mem_buffer:
            self.mem_buffer[path] = []
        self.mem_buffer[path] += [comment]

    def bufferFlush(self):
        for path in self.mem_buffer:
            with open(path, 'a+') as outfile:
                for comment in self.mem_buffer[path]:
                    json.dump(comment, outfile)
                    outfile.write("\n")
        self.mem_buffer = {}

    def storeDataForUser(self, comment):
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
        self.lang_model.update(commentText)

        self.bufferAdd(os.path.join(self.PROC_DATA_PATH,str(comment["author"])), comment)

    def storeComment(self, regularUsers, comment):
        if len(comment['children']) > 0:
            for child in comment['children']:
                self.storeComment(regularUsers, child)
        if (comment["author"] in regularUsers):
            if (comment.get("selftext") != "None" and \
                    comment.get("selftext")!= '') or \
                    (comment.get("body") != "None" and comment.get("body")!= ''):
                self.storeDataForUser(comment)

    def calcCommentsPerAuthor(self, comment, numCommentsPerAuthor):
        """
        Calculate number of comments per author
        """
        if len(comment['children']) > 0:
            for child in comment['children']:
                self.calcCommentsPerAuthor(child, numCommentsPerAuthor)
        if (comment["author"] != "[deleted]" and \
                comment["author"] != None and \
                comment["author"] != '') and \
                ((comment.get("selftext") != "None" and comment.get("selftext")!= '') \
                  or (comment.get("body") != "None" and comment.get("body")!= '')) :
            author = comment['author']
            n = numCommentsPerAuthor.get(author, 0)
            numCommentsPerAuthor[author] = n+1

    def findAndStoreRegularUsers(self):
        regularUsers = []
        self.lang_model = LM()
        if os.path.isfile(self.REGUSERLIST) and os.path.isfile(self.LM_PATH):
            print 'Reading REGUSERLIST'
            with open(self.REGUSERLIST, 'r') as f:
                regularUsers = [name.rstrip('\n') for name in f.readlines()]
            print 'Reading LM_PATH'
            with open(self.LM_PATH, 'r') as f:
                self.lang_model = pickle.load(f)
            return regularUsers

        shutil.rmtree(self.PROC_DATA_PATH, ignore_errors=True)
        os.mkdir(self.PROC_DATA_PATH)
        for fileName in os.listdir(self.RAW_DATA_PATH):
            if not fileName.startswith(self.subreddit+'.'):
                continue 
            print 'Reading :', fileName
            if fileName.endswith(".txt") or fileName.endswith(".jsonlist"):
                # load the json List from every fileName
                jsonList = json.load(
                        open(os.path.join(self.RAW_DATA_PATH, fileName)),
                        cls=ConcatJSONDecoder)
                print "file loaded."
                numCommentsPerAuthor = dict()
                for comment in jsonList:
                    self.calcCommentsPerAuthor(comment, numCommentsPerAuthor)

                numCommentsByRegUsers = 0
                totalNumComments = 0
                for author in numCommentsPerAuthor.keys():
                    numComments = numCommentsPerAuthor[author]
                    totalNumComments += numComments
                    if numComments > 25:
                        regularUsers.append(author)
                        numCommentsByRegUsers += numComments

                print "Total number of users:", len(numCommentsPerAuthor)
                print "Total number of comments by all users:", totalNumComments
                print "Number of users who had comments > 25:", len(regularUsers)
                print "Total number of comments by such users:", numCommentsByRegUsers
                print "------------------"

        print "Storing comments for regular users"
        for fileName in os.listdir(self.RAW_DATA_PATH):
            if not fileName.startswith(self.subreddit+'.'):
                continue
            if fileName.endswith(".txt") or fileName.endswith(".jsonlist"):
                jsonList = json.load(
                       open(os.path.join(self.RAW_DATA_PATH, fileName)),
                       cls=ConcatJSONDecoder)
                for comment in jsonList:
                    self.storeComment(regularUsers, comment)
        self.bufferFlush()
        print "storing done"
        with open(self.REGUSERLIST, 'w') as f:
            f.write('\n'.join(regularUsers))
        with open(self.LM_PATH, 'w') as f:
            pickle.dump(self.lang_model, f)
        return regularUsers

    def isActiveAfterSOFFor(self, commentDates, sofTime, n):
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

    def median(self, s):
        i = len(s)
        l = sorted(s)
        if not i%2:
            return floor((l[(i/2)-1]+l[i/2])/2.0)
        return l[i/2]

    def lenSubComments(self, comment):
        n = len(comment['children'])
        for c in comment['children']:
            n += self.lenSubComments(c)
        return n

    def worker(self, response):
        selftext = response.get('selftext', None)
        if selftext is not None:
            return vaderSentiment(selftext.encode('utf-8'))['compound']
        return 0


    def getSentimentResponse(self, comment):
        """
        For each comment in comments, return the sentiment of direct responses
        """
        sentimentPerComment = []
        for response in comment['children']:
            sentimentPerComment.append(self.worker(response))
        return sentimentPerComment

    def lm_init(self):
        if self.lang_model == None:
            self.lang_model = LM()
            if os.path.isfile(self.LM_PATH):
                with open(self.LM_PATH, 'r') as f:
                    self.lang_model = pickle.load(f)

    def findUsersWhoQuit(self, regularUsers):
        """
        Get users who quit
        """
        stats = dict()
        activeUsers = []
        quitters = []
        commentDates = dict()
        lastCommentDates = dict()
        firstCommentDates = dict()
        numAvgResponses = dict()
        responseSentiment = dict()
        commentSentiment = dict()
        numComNoResp = dict()
        fracComNoResp = dict()
        comTxtBeforeSOF = dict()
        allcommentsForUser = dict()
        print 'Reading comments for users'
        for user in regularUsers:
            print '.',
            comments = json.load(
                    open(os.path.join(self.PROC_DATA_PATH, user)),
                    cls=ConcatJSONDecoder)

            commentDatesList = []
            allcommentsForUser[user] = []
            for comment in comments:
                allcommentsForUser[user].append(comment)
                commentDatesList.append(float(comment['created_utc']))
            commentDates[user] = commentDatesList
            lastCommentDates[user] = max(commentDatesList)
            firstCommentDates[user] = min(commentDatesList)

        sofTimets = self.median(lastCommentDates.values())
        sofTime = datetime.utcfromtimestamp(sofTimets)
        stats['SOF_time_ts'] = sofTimets
        print "\nConsidering only the comments before SOF", sofTime
        print "Ignoring users with first comment after SOF"
        for user in regularUsers:
            if firstCommentDates[user] > sofTimets:
                continue
            if self.isActiveAfterSOFFor(commentDates[user], sofTime, 3):
                activeUsers.append(user)
            else:
                quitters.append(user)
            comTxtBeforeSOF[user] = [comment["selftext"] for comment in allcommentsForUser[user] if float(comment['created_utc']) <= sofTimets]

        quitters = quitters[:len(activeUsers)]
        stats['num_Q'] = len(quitters)
        stats['num_A'] = len(activeUsers)
        regularUsers = quitters + activeUsers
        if len(regularUsers) == 0:
            return None

        #'''
        for user in regularUsers:
            responseSentiment[user] = []
            commentSentiment[user] = []
            numComNoResp[user] = 0
            nres = 0
            ncom = 0
            for comment in allcommentsForUser[user]:
                if float(comment['created_utc']) > sofTimets:
                    continue
                numSubComments = self.lenSubComments(comment)
                if numSubComments == 0:
                    numComNoResp[user]+=1
                nres += numSubComments
                ncom += 1
                if self.SENT_ANALYSIS:
                    responseSentiment[user].append(self.getSentimentResponse(comment))
                    commentSentiment[user].append(vaderSentiment(comment.get('selftext').encode('utf-8')))
            numAvgResponses[user] = nres/ncom
            fracComNoResp[user] = float(numComNoResp[user])/ncom

        stats['Q_frac_comments_with_no_replies_avg'] = numpy.mean([fracComNoResp[user] for user in quitters])
        stats['Q_frac_comments_with_no_replies_stddev'] = numpy.std([fracComNoResp[user] for user in quitters])
        stats['A_frac_comments_with_no_replies_avg'] = numpy.mean([fracComNoResp[user] for user in activeUsers])
        stats['A_frac_comments_with_no_replies_stddev'] = numpy.std([fracComNoResp[user] for user in activeUsers])

        if self.SENT_ANALYSIS:
            '''
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
            '''
            stats['Q_comment_sent_comp_avg'] = numpy.mean([csent['compound'] for csent in [commentSentiment[user] for user in quitters]])
            stats['Q_comment_sent_comp_stddev'] = numpy.std([csent['compound'] for csent in [commentSentiment[user] for user in quitters]])
            stats['Q_response_sent_comp_avg'] = numpy.mean([self.average(responseSentiment[user]) for user in quitters])
            stats['Q_response_sent_comp_stddev'] = numpy.std([self.average(responseSentiment[user]) for user in quitters])
            stats['Q_sent_corr_avg'] = numpy.mean([self.measure_correlation(commentSentiment[user], responseSentiment[user]) for user in quitters])
            stats['Q_sent_corr_stddev'] = numpy.std([self.measure_correlation(commentSentiment[user], responseSentiment[user]) for user in quitters])

            stats['A_comment_sent_comp_avg'] = numpy.mean([csent['compound'] for csent in [commentSentiment[user] for user in activeUsers]])
            stats['A_comment_sent_comp_stddev'] = numpy.std([csent['compound'] for csent in [commentSentiment[user] for user in activeUsers]])
            stats['A_response_sent_comp_avg'] = numpy.mean([self.average(responseSentiment[user]) for user in activeUsers])
            stats['A_response_sent_comp_stddev'] = numpy.std([self.average(responseSentiment[user]) for user in activeUsers])
            stats['A_sent_corr_avg'] = numpy.mean([self.measure_correlation(commentSentiment[user], responseSentiment[user]) for user in activeUsers])
            stats['A_sent_corr_stddev'] = numpy.std([self.measure_correlation(commentSentiment[user], responseSentiment[user]) for user in activeUsers])

            '''
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
            '''
        stats['Q_num_response_avg'] = numpy.mean([numAvgResponses[user] for user in quitters])
        stats['Q_num_response_stddev'] = numpy.std([numAvgResponses[user] for user in quitters])
        stats['A_num_response_avg'] = numpy.mean([numAvgResponses[user] for user in activeUsers])
        stats['A_num_response_stddev'] = numpy.std([numAvgResponses[user] for user in activeUsers])

        avgNumComQ = 0
        avgNumComA = 0
        avgNumCom = 0
        for user in quitters:
            avgNumCom += len(comTxtBeforeSOF[user])
            avgNumComQ += len(comTxtBeforeSOF[user])
        for user in activeUsers:
            avgNumCom += len(comTxtBeforeSOF[user])
            avgNumComA += len(comTxtBeforeSOF[user])
        print 'Num of comments (R Q A): ', avgNumCom, avgNumComQ, avgNumComA
        print 'Num users (R Q A): ', len(quitters)+len(activeUsers), len(quitters),len(activeUsers)
        avgNumCom = avgNumCom/(len(quitters) + len(activeUsers))
        avgNumComQ = avgNumComQ/len(quitters)
        avgNumComA = avgNumComA/len(activeUsers)
        print 'Avg num of comments: (R Q A) ', avgNumCom, avgNumComQ, avgNumComA
        stats['Q_num_comments_per_user_avg'] = numpy.mean([len(comTxtBeforeSOF[user]) for user in quitters])
        stats['Q_num_comments_per_user_stddev'] = numpy.std([len(comTxtBeforeSOF[user]) for user in quitters])
        stats['A_num_comments_per_user_avg'] = numpy.mean([len(comTxtBeforeSOF[user]) for user in activeUsers])
        stats['A_num_comments_per_user_stddev'] = numpy.std([len(comTxtBeforeSOF[user]) for user in activeUsers])
     
        minAvgNumCom = min(avgNumComQ, avgNumComA)

        print "Language modeling"
        uniqueWords = dict()
        numCommentsConsidered = dict()
        totalWordsConsidered = dict()
        uniqueWordsFraction = dict()
        commentLength = dict()
        jsdiv = dict()
        kldiv = dict()
        nComQ = 0
        nWQ = 0
        nComA = 0
        nWA = 0
        Q = []
        A = []

        comTxtBeforeSOFQ = [comTxtBeforeSOF[user][:minAvgNumCom] for user in quitters]
        comTxtBeforeSOFA = [comTxtBeforeSOF[user][:minAvgNumCom] for user in activeUsers]
        #pool = mp.Pool(initializer = self.lm_init)
        #ugramQ = pool.map(self.ugramModel, comTxtBeforeSOFQ)
        #ugramA = pool.map(self.ugramModel, comTxtBeforeSOFA)
        for idx, user in enumerate(quitters):
            # num_uw, frac_uw, num_tw_considered, num_com_considered, comment_len, js_div, kl_div = ugramQ[idx]
            num_uw, frac_uw, num_tw_considered, num_com_considered, comment_len, js_div, kl_div = self.ugramModel(comTxtBeforeSOFQ[idx])
            uniqueWords[user] = num_uw
            totalWordsConsidered[user] = num_tw_considered
            numCommentsConsidered[user] = num_com_considered
            uniqueWordsFraction[user] = frac_uw
            commentLength[user] = comment_len
            jsdiv[user] = js_div
            kldiv[user] = kl_div
            nComQ += num_com_considered
            nWQ += num_tw_considered
            Q.append(user)

        for idx, user in enumerate(activeUsers):
            #if user not in comTxtBeforeSOF.keys():
            #    continue
            #num_uw, frac_uw, num_tw_considered, num_com_considered, comment_len, js_div, kl_div = ugramA[idx]
            num_uw, frac_uw, num_tw_considered, num_com_considered, comment_len, js_div, kl_div = self.ugramModel(comTxtBeforeSOFA[idx])
            uniqueWords[user] = num_uw
            totalWordsConsidered[user] = num_tw_considered
            numCommentsConsidered[user] = num_com_considered
            uniqueWordsFraction[user] = frac_uw
            commentLength[user] = comment_len
            jsdiv[user] = js_div
            kldiv[user] = kl_div
            nComA += num_com_considered
            nWA += num_tw_considered
            A.append(user)
            if nComQ < nComA:
                break
        print nComQ, nComA, nWQ, nWA

        stats['total_num_comments_considered_per_type'] = nComQ

        stats['Q_comments_length_per_user_avg'] = numpy.average([commentLength[user] for user in Q])
        stats['Q_comments_length_per_user_stddev'] = numpy.std([commentLength[user] for user in Q])
        stats['A_comments_length_per_user_avg'] = numpy.average([commentLength[user] for user in A])
        stats['A_comments_length_per_user_stddev'] = numpy.std([commentLength[user] for user in A])

        stats['Q_words_considered_per_user_avg'] = numpy.average([totalWordsConsidered[user] for user in Q])
        stats['Q_words_considered_per_user_stddev'] = numpy.std([totalWordsConsidered[user] for user in Q])
        stats['A_words_considered_per_user_avg'] = numpy.average([totalWordsConsidered[user] for user in A])
        stats['A_words_considered_per_user_stddev'] = numpy.std([totalWordsConsidered[user] for user in A])

        stats['Q_comments_considered_per_user_avg'] = numpy.average([numCommentsConsidered[user] for user in Q])
        stats['Q_comments_considered_per_user_stddev'] = numpy.std([numCommentsConsidered[user] for user in Q])
        stats['A_comments_considered_per_user_avg'] = numpy.average([numCommentsConsidered[user] for user in A])
        stats['A_comments_considered_per_user_stddev'] = numpy.std([numCommentsConsidered[user] for user in A])

        stats['Q_num_unique_words_per_user_avg'] = numpy.average([uniqueWords[user] for user in Q])
        stats['Q_num_unique_words_per_user_stddev'] = numpy.std([uniqueWords[user] for user in Q])
        stats['A_num_unique_words_per_user_avg'] = numpy.average([uniqueWords[user] for user in A])
        stats['A_num_unique_words_per_user_stddev'] = numpy.std([uniqueWords[user] for user in A])

        stats['Q_frac_unique_words_per_user_avg'] = numpy.average([uniqueWordsFraction[user] for user in Q])
        stats['Q_frac_unique_words_per_user_stddev'] = numpy.std([uniqueWordsFraction[user] for user in Q])
        stats['A_frac_unique_words_per_user_avg'] = numpy.average([uniqueWordsFraction[user] for user in A])
        stats['A_frac_unique_words_per_user_stddev'] = numpy.std([uniqueWordsFraction[user] for user in A])

        stats['Q_JS_divergence_avg'] = numpy.average([jsdiv[user] for user in Q])
        stats['Q_JS_divergence_stddev'] = numpy.std([jsdiv[user] for user in Q])
        stats['Q_KL_divergence_avg'] = numpy.average([kldiv[user] for user in Q])
        stats['Q_KL_divergence_stddev'] = numpy.std([kldiv[user] for user in Q])

        stats['A_JS_divergence_avg'] = numpy.average([jsdiv[user] for user in A])
        stats['A_JS_divergence_stddev'] = numpy.std([jsdiv[user] for user in A])
        stats['A_KL_divergence_avg'] = numpy.average([kldiv[user] for user in A])
        stats['A_KL_divergence_stddev'] = numpy.std([kldiv[user] for user in A])
        self.print_stats(stats)
        
        '''

        print "Q mrc :"
        
        mrcvQ = [0 for i in range(14)]
        cnt = 0
        for user in quitters:
            cnt += 1
            v = self.mrcPrep(comTxtBeforeSOF[user])
            for i in range(14):
                mrcvQ[i] += v[i]
        for i in range(14):
            print mrcvQ[i] / float(cnt)

        print "A mrc :"
            
        mrcvA = [0 for i in range(14)]
        cnt = 0
        for user in activeUsers:
            cnt += 1
            v = self.mrcPrep(comTxtBeforeSOF[user])
            for i in range(14):
                mrcvA[i] += v[i]
        for i in range(14):
            print mrcvA[i] / float(cnt)
        '''
        
    def mrcPrep(self, comments):
        print '.',
        sys.stdout.flush()
        cnt = 0
        val = [0 for i in range(14)]
        
        for comment in comments:
            text = nltk.Text(self.word_punct_tokenizer.tokenize(comment.lower()))
            for x in text:
                cnt += 1
                v = self.mrc.query(x)
                for i in range(14):
                    val[i] += v[i]
        if cnt > 0:
            for i in range(14):
                val[i] /= float(cnt)
        return val

    def ugramModel(self, comments):
        print '.',
        sys.stdout.flush()
        commentTokens = list()
        js_div = self.lang_model.jsdivergence(' '.join(comments))
        kl_div = self.lang_model.kldivergence(' '.join(comments))
        comment_len = 0.0
        for comment in comments:
            text = nltk.Text(self.word_punct_tokenizer.tokenize(comment.lower()))
            tempList = [i for i in text if i not in self.stop]
            comment_len += len(tempList)
            commentTokens.extend(tempList[:40])
        num_uniq_words = len(set(commentTokens))
        frac_uniq_words = float(len(set(commentTokens)))/(len(commentTokens)+1)
        avg_comment_len = comment_len/len(comments)
        return (num_uniq_words, frac_uniq_words, len(commentTokens), len(comments), avg_comment_len, js_div, kl_div)

    def measure_correlation(self, comment_sent, response_sent_list):
        avg_response_sent = []
        compound_comment_sent = [x['compound'] for x in comment_sent]
        for lst in response_sent_list:
            avg_response_sent.append(numpy.average(lst) if len(lst) > 0 else 0)
        return numpy.cov(compound_comment_sent, avg_response_sent)[0, 1]

    def average(self, ll):
        n = 0
        sum = 0
        for l in ll:
            n += len(l)
            for x in l:
                sum += x
        if n==0:
            return 0
        return sum/n

    def start(self):
        self.mrc = MRC()
        self.mrc.init()
        regularUsers = self.findAndStoreRegularUsers()
        print len(regularUsers)
        self.findUsersWhoQuit(regularUsers)
        print '---- ' + self.subreddit

    def print_stats(self, stats):
        l.acquire()
        ss = OrderedDict(sorted(stats.items()))
        if not os.path.isfile(self.STATS_FILE):
            with open(self.STATS_FILE, 'w') as f:
                f.write('Subreddit\t' + '\t'.join(ss.keys()) + '\n')
        with open(self.STATS_FILE, 'a') as f:
            f.write(self.subreddit + '\t' + '\t'.join([str(x) for x in ss.values()]) + '\n')
        l.release()

def process_subreddit(subreddit):
    global l
    print '++++ ' + subreddit
    l.acquire()
    global_data.total_started += 1
    global_data.total_running += 1
    print 'Waiting for : ', global_data.total_running, '/', global_data.total_started
    l.release()

    model = Model(subreddit)
    model.start()
    l.acquire()
    global_data.total_running -= 1
    print 'Waiting for : ', global_data.total_running, '/', global_data.total_started
    l.release()
  

if __name__=="__main__":
    subreddits = []
    for fileName in os.listdir(RAW_DATA_PATH):
        if fileName.endswith('jsonlist'):
            subreddits.append(fileName.split('.')[0])
    pool = mp.Pool()
    pool.map(process_subreddit, subreddits)
