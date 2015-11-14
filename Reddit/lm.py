from __future__ import division
import math
from nltk.tokenize import WordPunctTokenizer
from nltk.corpus import stopwords
import nltk

tokenizer = WordPunctTokenizer()
stop = stopwords.words('english')

class LM:
    def __init__(self):
        self.num_grams = 0
        self.tf = dict()
        self.n = 2
        self.alpha = 1

    def update(self, text):
        tokens = [w for w in nltk.Text(tokenizer.tokenize(text.lower())) if w not in stop]
        ngram_list = [tuple(tokens[i:i+self.n]) for i in xrange(len(tokens)-self.n)]
        self.num_grams += len(ngram_list)
        for gram in ngram_list:
            self.tf[gram] = self.tf.get(gram, 0) + 1

    def prob(self, gram):
        return (self.tf.get(gram, 0)+self.alpha)/(self.num_grams + self.alpha * len(self.tf))

    def entropy(self):
        e = 0
        for gram,_ in self.tf.iteritems():
            p = self.prob(gram)
            e += -p * math.log(p, 2)
        return e

    def distribution(self, text):
        prob = dict()
        freq = dict()
        tokens = [w for w in nltk.Text(tokenizer.tokenize(text.lower())) if w not in stop]
        ngram_list = [tuple(tokens[i:i+self.n]) for i in xrange(len(tokens)-self.n)]
        N = len(ngram_list)
        d = len(self.tf)
        for gram in ngram_list:
            freq[gram] = freq.get(gram, 0) + 1
        for gram,_ in self.tf.iteritems():
            prob[gram] = (freq.get(gram, 0) + self.alpha)/(N + self.alpha * d)
        return prob

    def crossentropy(self, text):
        qprob = self.distribution(text)
        e = 0
        for gram,_ in self.tf.iteritems():
            p = self.prob(gram)
            q = qprob[gram]
            e += -p * math.log(q, 2)
        return e

    def kldivergence(self, text):
        qprob = self.distribution(text)
        kldiv = 0
        for gram,_ in self.tf.iteritems():
            p = self.prob(gram)
            q = qprob[gram]
            kldiv += -p * math.log(p/q, 2)
        return kldiv

    def jsdivergence(self, text):
        qprob = self.distribution(text)
        jsdiv = 0
        for gram,_ in self.tf.iteritems():
            p = self.prob(gram)
            q = qprob[gram]
            m = (p+q)/2
            jsdiv += (-p * math.log(p/m, 2)) + (-q * math.log(q/m, 2))
        return jsdiv


def main():
    lm = LM()
    sentence = "A wiki is run using wiki software, otherwise known as a wiki engine. There are dozens of different wiki engines in use, both standalone and part of other software, such as bug tracking systems. Some wiki engines are open source, whereas others are proprietary. Some permit control over different functions (levels of access); for example, editing rights may permit changing, adding or removing material. Others may permit access without enforcing access control. Other rules may also be imposed to organize content. A wiki engine is a type of content management system, but it differs from most other such systems, including blog software, in that the content is created without any defined owner or leader, and wikis have little implicit structure, allowing structure to emerge according to the needs of the users"

    lm.update(sentence)
    #lm.update(" ".join(sentence.split()[1:]))
    #lm.update(" ".join(sentence.split()[2:]))
    x = 0
    for w,f in lm.tf.iteritems():
        p = f/lm.num_grams
        x+=p
    print x

    print "Entropy:", lm.entropy()
    print "Self cross entropy:", lm.crossentropy(sentence)
    print "Self KL divergence:", lm.kldivergence(sentence)
    print "Self JS divergence:", lm.jsdivergence(sentence)
    reverse_sen = list(sentence.split())
    reverse_sen.reverse()
    print "cross entropy:", lm.crossentropy(' '.join(reverse_sen))
    print "KL divergence:", lm.kldivergence(' '.join(reverse_sen))
    print "JS divergence:", lm.jsdivergence(' '.join(reverse_sen))


if __name__ == "__main__":
    main()
