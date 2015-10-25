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

        objs = []
        end = 0
        while end != s_len:
            obj, end = self.raw_decode(s, idx=_w(s, end).end())
            end = _w(s, end).end()
            objs.append(obj)
        return objs

def storeDataForUser(jsonObj):

    # get a reference to the table 'user'
    # table = db[(jsonObj["author"])]

    print jsonObj.get("selftext")
    if jsonObj.get("selftext") !=  None :
        commentText = jsonObj["selftext"]
    elif jsonObj.get("body") != '':
        commentText = jsonObj["body"]
        print jsonObj
    else:
        commentText = 'None'

    tableDict = dict()
    tableDict["subreddit"] = (jsonObj["subreddit"]) if (jsonObj["subreddit"]) != '' else 'None'
    tableDict["subreddit_id"] = (jsonObj["subreddit_id"]) if (jsonObj["subreddit_id"]) != '' else 'None'
    tableDict["author"] = (jsonObj["author"]) if (jsonObj["author"]) != '' else 'None'
    tableDict["comment_id"] = (jsonObj["id"]) if (jsonObj["author"]) != '' else 'None'
    tableDict["selftext"] = commentText
    tableDict["title"] = (jsonObj["title"]) if (jsonObj["title"]) != '' else 'None'
    tableDict["likes"] = (jsonObj["likes"]) if (jsonObj["likes"]) != '' else 'None'
    tableDict["downs"] = (jsonObj["downs"]) if (jsonObj["downs"]) != '' else 'None'
    tableDict["num_comments"] = (jsonObj["num_comments"]) if (jsonObj["num_comments"]) != '' else 'None'
    tableDict["ups"] = (jsonObj["ups"]) if (jsonObj["ups"]) != '' else 'None'
    tableDict["created_utc"] = (jsonObj["created_utc"]) if (jsonObj["created_utc"]) != '' else 'None'

    with open(os.path.join(processedDataPath,str(tableDict["author"])), 'a+') as outfile:
        json.dump(tableDict,outfile)
    # Insert a new record.
    # table.insert(tableDict)

#path = sys.argv[1]
#path2 = sys.argv[2]
path = rawDataPath
print(path)

def recursionForStoringData(jsonObj):

    if len(jsonObj['children']) > 0:
        for child in jsonObj['children']:
            recursionForStoringData(child)
    else:
        if (jsonObj["author"] in totalUsers):
            if (jsonObj.get("selftext") != "None" and jsonObj.get("selftext")!= '') or (jsonObj.get("body") != "None" and jsonObj.get("body")!= ''):
                storeDataForUser(jsonObj)


def populateUserList(jsonObj):


    if len(jsonObj['children']) > 0:
        for child in jsonObj['children']:
            populateUserList(child)
    else:
        if (jsonObj["author"] != "[deleted]" and jsonObj["author"] != None and jsonObj["author"] != '') and ((jsonObj.get("selftext") != "None" and jsonObj.get("selftext")!= '') or (jsonObj.get("body") != "None" and jsonObj.get("body")!= '')) :
            originalUserList.append(jsonObj["author"])

originalUserList = list()
totalUsers = set()
def main():


    # walk the directory and read the files
    # Make sure you have all the raw data files in folder
    # RawData...


    # find out the list of users to get the data for
    for file in os.listdir(path):
        if file.endswith(".txt") or file.endswith(".jsonlist"):
            # load the json List from every file

            jsonList = json.load(open(os.path.join(path,file)),cls=ConcatJSONDecoder)
            # From the JsonList populate the DB for every user

            print "-------------------"
            print file

            for jsonObj in jsonList:
                populateUserList(jsonObj)



            userDict = dict()
            for i in originalUserList:
                userDict[i] = userDict.get(i, 0) + 1

            newDict = dict()
            numOfUsers = len(userDict.keys())
            #print "total unique users before =" + str(numOfUsers)
            for key in userDict:
                if userDict[key] > 20:
                    newDict[key] = userDict[key]

            posts = 0
            for value in newDict.values():
                posts+=value

            if len(newDict.keys())>0:
                for user in newDict.keys():
                    totalUsers.add(user)

            print "total number of users who had posts>20 is" + str(len(newDict.keys()))
            print "total number of posts " + str(posts)
            print "------------------"
    print "Total number of users collected is : "+str(len(totalUsers))

    print "Now populating data"
    count = 0
    for file in os.listdir(path):
        if file.endswith(".txt") or file.endswith(".jsonlist"):
            # load the json List from every file
            jsonList = json.load(open(os.path.join(path,file)),cls=ConcatJSONDecoder)
            # From the JsonList populate the DB for every user
            for jsonObj in jsonList:
                recursionForStoringData(jsonObj)




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