import json
import os
import random
import re
from vaderSentiment.vaderSentiment import sentiment as vaderSentiment

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

def analyse():
    f = open("label.json", "r")
    data = json.load(f)
    f.close()
    print len(data)
    cnt1 = {'a':0, 'b':0, 'c':0}
    cnt2 = {'-':0, '0':0, '+':0}
    cor = {'a':0, 'b':0, 'c':0}
    avgl = 0
    for i in xrange(len(data)):
        #if data[i][2]["neg"] == data[i][2]["pos"]: continue
        avgl += len(data[i][1].split('.'))
        cnt1[data[i][0]] += 1
        if data[i][2]["compound"] > 0 and data[i][0] == 'c':
            cor['c'] += 1
        elif data[i][2]["compound"] < 0 and data[i][0] == 'a':
            cor['a'] += 1
        elif data[i][2]["compound"] == 0 and data[i][0] == 'b':
            cor['b'] += 1
            
        if data[i][2]["compound"] < 0:
            cnt2['-'] += 1
        elif data[i][2]["compound"] == 0:
            cnt2['0'] += 1
        else:
            cnt2['+'] += 1

        s = {'a':-1, 'b':0, 'c':1}
        #print str(s[data[i][0]]), str(data[i][2]["compound"])

    print float(avgl) / len(data)
    print cnt1
    #print cnt2
    print cor

def main():
    lis = os.listdir("ProcessedData")
    text = []
    for fn in lis:
        f = open("ProcessedData\\"+fn, "r")
        data = json.load(f, cls=ConcatJSONDecoder)
        f.close()
        for d in data:
            text += [d["selftext"]]
            pass
    print 'Total:', len(text)
    random.shuffle(text)
    ld = []
    p = 0
    vis = {}
    try:
        f = open("label.json", "r")
        ld = json.load(f)
        f.close()
        for x in ld:
            vis[x[1]] = True
    except:
        pass
    while True:
        if p >= len(text): break
        if text[p] is not None and text[p] not in vis:
            print text[p]
            r = raw_input('a. -1  b. 0  c. 1')
            ld += [(r, text[p], vaderSentiment(text[p].encode('utf-8')))]
            vis[text[p]] = True
            print ld[-1][0], ld[-1][2]
            print '\n\n\n====================\n'
        f = open("label.json", "w")
        json.dump(ld, f, indent=1)
        f.close()
        p += 1
    

if __name__ == '__main__':
    analyse()
    main()
