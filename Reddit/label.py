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
    f = open("label_a.json", "r")
    data = json.load(f)
    f.close()
    cnt = {'a':0, 'b':0, 'c':0}
    cor = {'a':0, 'b':0, 'c':0}
    for i in xrange(len(data)):
        if data[i][2]["neg"] == data[i][2]["pos"]: continue
        cnt[data[i][0]] += 1
        if data[i][2]["compound"] > 0 and data[i][0] == 'c':
            cor['c'] += 1
        elif data[i][2]["compound"] < 0 and data[i][0] == 'a':
            cor['a'] += 1
    print cnt
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
    while True:
        if p >= len(text): break
        if text[p] is not None:
            print text[p]
            r = raw_input('a. -1  b. 0  c. 1')
            ld += [(r, text[p], vaderSentiment(text[p].encode('utf-8')))]
            print ld[-1][0], ld[-1][2]
            print '\n\n\n====================\n'
        f = open("label.json", "w")
        json.dump(ld, f, indent=1)
        f.close()
        p += 1
    

if __name__ == '__main__':
    #main()
    analyse()
