from sklearn.preprocessing import normalize
from sklearn.base import BaseEstimator
from collections import defaultdict
from time import time
from math import log

class HF_IHU_URL(BaseEstimator):

    def __init__(self) :
        self.hfm = defaultdict(int)
        self.thfm = defaultdict(lambda: defaultdict(int))
        self.corpus_size = 0

    def fit(self, terms, hashtags):
        self.corpus_size+=len(terms)
        if not hashtags:
            return

        for ht in hashtags:
            self.hfm[ht] += 1

        for t in terms:
            for ht in hashtags:
                self.thfm[t][ht]+=1


    def predict(self, terms, p):
        sh = defaultdict(int)
        for t in terms:
            ht_co_occur = [*self.thfm[t]]
            thfm_sum = 0
            for ht in ht_co_occur:
                thfm_sum += self.thfm[t][ht]

            for ht in ht_co_occur:
                hfthj = self.thfm[t][ht]/thfm_sum
                ihuh = log (self.corpus_size/self.hfm[ht])
                sh[ht]+=hfthj*ihuh

        a = {key: sh[key] for key in sorted(sh, key=sh.get, reverse=True)[:p]}
        b = a.keys()
        return b
        #return {key: sh[key] for key in sorted(sh, key=sh.get, reverse=True)[:p]}


