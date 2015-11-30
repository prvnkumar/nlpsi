class MRC():
    def init(self):
        self.data = {}
        f = open("MRC/mrc2.dct", "r")
        line = f.readline()
        while line:
            word = line[51:].split('|')[0].lower()
            if word not in self.data:
                self.data[word] = []
            self.data[word] += [line[:51]]
            line = f.readline()
        f.close()
        ok = [True for i in range(51)]
        for word in self.data:
            if len(self.data[word]) == 1: continue
            for i in range(51):
                if not ok[i]: continue
                for j in range(1, len(self.data[word])):
                    if self.data[word][j][i] != self.data[word][0][i]:
                        ok[i] = False
                        break
        #print ok
        #1     2     3      4      5
        #True, True, False, False, False,
        #6     7     8      9      10
        #True, True, False, False, False,
        #11    12    13     14     15
        #False, False, False, False, False,
        #16    17    18     19     20
        #True, True, False, False, False,
        #21    22    23     24     25
        #False, True, True, True, True,
        #26    27    28     29     30
        #False, False, False, False, False,
        #31    32    33     34     35
        #False, False, False, False, False,
        #36    37    38     39     40
        #False, False, True, True, True,
        #41    42    43     44     45
        #False, False, False, False, False,
        #46    47    48     49     50
        #False, False, False, False, True,
        #51
        #False

    def query_all(self, word):
        return self.data[word.lower()]

    def query(self, word):
        if word not in self.data:
            return [0 for i in range(14)]
        return [self.query_nlet(word), \
               self.query_nphon(word), \
               self.query_nsyl(word), \
               self.query_kffreq(word), \
               self.query_kfncats(word), \
               self.query_kfnsamp(word), \
               self.query_tlfreq(word), \
               self.query_brownfreq(word), \
               self.query_fam(word), \
               self.query_conc(word), \
               self.query_imag(word), \
               self.query_meanc(word), \
               self.query_meanp(word), \
               self.query_aoa(word)]

    def query_nlet(self, word):
        return int(self.data[word.lower()][0][0:2])

    def query_nphon(self, word): #average
        word = word.lower()
        ret = 0
        for i in range(len(self.data[word])):
            ret += int(self.data[word][i][2:4])
        return ret / float(len(self.data[word]))

    def query_nsyl(self, word): #average
        word = word.lower()
        ret = 0
        for i in range(len(self.data[word])):
            ret += int(self.data[word][i][4:5])
        return ret / float(len(self.data[word]))

    def query_kffreq(self, word): #average
        word = word.lower()
        ret = 0
        for i in range(len(self.data[word])):
            ret += int(self.data[word][i][5:10])
        return ret / float(len(self.data[word]))

    def query_kfncats(self, word): #average
        word = word.lower()
        ret = 0
        for i in range(len(self.data[word])):
            ret += int(self.data[word][i][10:12])
        return ret / float(len(self.data[word]))

    def query_kfnsamp(self, word): #average
        word = word.lower()
        ret = 0
        for i in range(len(self.data[word])):
            ret += int(self.data[word][i][12:15])
        return ret / float(len(self.data[word]))

    def query_tlfreq(self, word): #average
        word = word.lower()
        ret = 0
        for i in range(len(self.data[word])):
            ret += int(self.data[word][i][15:21])
        return ret / float(len(self.data[word]))

    def query_brownfreq(self, word):
        return int(self.data[word.lower()][0][21:25])

    def query_fam(self, word): #average
        word = word.lower()
        ret = 0
        for i in range(len(self.data[word])):
            ret += int(self.data[word][i][25:28])
        return ret / float(len(self.data[word]))

    def query_conc(self, word): #average
        word = word.lower()
        ret = 0
        for i in range(len(self.data[word])):
            ret += int(self.data[word][i][28:31])
        return ret / float(len(self.data[word]))

    def query_imag(self, word): #average
        word = word.lower()
        ret = 0
        for i in range(len(self.data[word])):
            ret += int(self.data[word][i][31:34])
        return ret / float(len(self.data[word]))

    def query_meanc(self, word): #average
        word = word.lower()
        ret = 0
        for i in range(len(self.data[word])):
            ret += int(self.data[word][i][34:37])
        return ret / float(len(self.data[word]))

    def query_meanp(self, word): #average
        return int(self.data[word.lower()][0][37:40])

    def query_aoa(self, word): #average
        word = word.lower()
        ret = 0
        for i in range(len(self.data[word])):
            ret += int(self.data[word][i][40:43])
        return ret / float(len(self.data[word]))

if __name__ == '__main__':
    mrc = MRC()
    mrc.init()
    x = raw_input()
    print mrc.query(x)
