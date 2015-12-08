import math
#import numpy as np
import matplotlib.pyplot as plt

def main():
    fsize = {}
    f = open("fsize.txt", "r")
    x = f.readline()
    while x:
        x = x.split()
        x[1] = x[1].split('.')
        if x[1][1] == "jsonlist":
            fsize[x[1][0].lower()] = int(x[0])
        x = f.readline()
    f.close()
    
    f = open("stats_mrc.txt", "r")
    h = f.readline()
    h = h.split()[1:]
    r = {}
    x = f.readline()
    while x:
        x = x.split()
        t = x[0]
        if t.lower() not in fsize or fsize[t.lower()] > 30000000:
        #if True:
            r[t] = {}
            x = x[1:]
            for i in xrange(len(h)):
                r[t][h[i]] = float(x[i])
        x = f.readline()
    f.close()
    
    stat = {}
    statavg = {}
    statvar = {}
    delta = {}
    for i in xrange(14):
        stat[i] = [0, 0]
        statavg[i] = [0, 0]
        statvar[i] = [0, 0]
        delta[i] = []
        for x in r:
            a = 'MRC_A_'+str(i)
            q = 'MRC_Q_'+str(i)
            if r[x][a] == 0 or r[x][q] == 0: continue
            if r[x][a] > r[x][q]:
                stat[i][0] += 1
            elif r[x][a] < r[x][q]:
                stat[i][1] += 1
            statavg[i][0] += r[x][a]
            statavg[i][1] += r[x][q]
            statvar[i][0] += r[x][a] ** 2
            statvar[i][1] += r[x][q] ** 2
            delta[i] += [(r[x][a] - r[x][q])] #[(r[x][a] - r[x][q]) / (r[x][a] + r[x][q])]
            
        statavg[i][0] /= float(len(r))
        statavg[i][1] /= float(len(r))
        statvar[i][0] = (statvar[i][0] / float(len(r)) - statavg[i][0] ** 2) ** 0.5
        statvar[i][1] = (statvar[i][1] / float(len(r)) - statavg[i][1] ** 2) ** 0.5
    print stat
    print
    print statavg
    print
    print statvar
    print
    print len(r)

    f = open("mrc.csv", "w")
    for i in xrange(14):
        delta[i].sort()
        for j in xrange(len(delta[i])):
            f.write(str(delta[i][j]) + ', ')
        f.write("\n")

    '''
    for i in xrange(14):
        for j in xrange(len(delta[i])):
            delta[i][j] /= max(abs(delta[i][0]), abs(delta[i][-1]))
        his = {}
        for j in xrange(len(delta[i])):
            k = math.floor(delta[i][j] * 40)
            if k not in his:
                his[k] = 0
            his[k] += 1
        for k in range(-20, 21):
            if k not in his: his[k] = 0
            f.write(str(his[k]) + ', ')
        f.write("\n")
    '''
    
    f.close()

    #plt.hist(delta[0], bins=45)
    #plt.hist(delta[1], bins=45)
    #plt.hist(delta[2], bins=45)
    #plt.hist(delta[3], bins=45)
    #plt.hist(delta[6], bins=45)
    plt.hist(delta[13], bins=45)
    plt.grid(True)
    plt.show()

if __name__ == '__main__':
    main()
