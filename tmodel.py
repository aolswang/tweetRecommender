from HF_IHU_URL import *
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import pickle


class tmodel:
    """This class is responsible for the evaluation of the the HF_IHU_URL model as described in Twitter Hashtag Recommendation System Using URL Information.
 :parameter
 test_size = the ratio between train and test
 url_support = orig model or enhanced model
 url_only = skip tweets without hashtags
 learn_without_url_test_with_url = learn without url, test with url

 It supports the following methods:
 1. init - read the data files according the configuartion.
 2. fit the model - run the fit with the train data
 3. test_the_model - run the predict across all the test data and return the results"
 4. print_model_information - print the config of the model and the sizes of its inner variables"""

    def __init__(self, test_size=0.1, seed=1,url_support=True, url_only=False, learn_without_url_test_with_url=False, data_directory="data/200000") :
        self.model = HF_IHU_URL()
        self.url_support = url_support
        self.url_only = url_only
        self.test_size = test_size
        self.seed = seed
        self.learn_without_url_test_with_url = learn_without_url_test_with_url
        print ("Data directory: {}".format(data_directory))
        
        ht = pd.read_csv(data_directory+"/hashtags.csv")
        terms = pd.read_csv(data_directory+"/terms.csv")
        urlterms = pd.read_csv(data_directory+"/urlterms.csv") if self.url_support else pd.DataFrame()

        if (self.learn_without_url_test_with_url) :
            self.ht_train, self.ht_test, self.terms_train, self.terms_test, self.urlterms_train, self.urlterms_test = train_test_split(
                ht, terms,
                urlterms,
                test_size=test_size,
                random_state=seed)
            self.urlterms_train = pd.DataFrame()
            print ("learn_without_url_test_with_url")
        elif (self.url_support) :
            if (url_only) :
                rows_with_urls = ~(urlterms[urlterms.columns[1]].isnull())
                ht = ht[rows_with_urls]
                terms = terms[rows_with_urls]
                urlterms=urlterms[rows_with_urls]
            self.ht_train, self.ht_test, self.terms_train, self.terms_test, self.urlterms_train, self.urlterms_test = train_test_split(ht, terms,
                                                                                                     urlterms,
                                                                                                     test_size=test_size,
                                                                                                     random_state=seed)
        else:
            self.ht_train, self.ht_test, self.terms_train, self.terms_test = train_test_split(ht, terms, test_size=test_size,
                random_state=seed)
            self.urlterms_train = []
            self.urlterms_test = []



        self.train_num_of_rows = self.ht_train.shape[0]
        self.test_num_of_rows = self.ht_test.shape[0]

    def read_row (self, k, ht_df, terms_df, urlterms_df) :
        hashtags = ht_df.iloc[k:k+1, 1:].dropna(axis=1)
        hashtags = hashtags.values.flatten().tolist()

        terms = terms_df.iloc[k:k+1, 1:].dropna(axis=1)
        if (self.url_support ):
            urlt = urlterms_df.iloc[k:k+1, 1:].dropna(axis=1)
            terms = pd.concat([terms, urlt], axis=1, ignore_index=True)

        terms = terms.values.flatten().tolist()
        return terms, hashtags

    def fit_the_model (self):
        for i in range(self.train_num_of_rows):
            terms, hashtags = self.read_row(i, self.ht_train, self.terms_train, self.urlterms_train)
            self.model.fit( terms, hashtags)
        print ("Finish fitting {} rows".format (self.train_num_of_rows))

    def recall (self, list1, list2):
        p = set(list1) & set(list2)
        return len(p)

    def test_the_model (self, max_p, test_with_url=True) :
        recall_ratio_arr = np.zeros(max_p)
        num_of_hashtags_in_testset = 0
        for i in range(self.test_num_of_rows):
            if (test_with_url):
                terms, test_hashtags = self.read_row(i, self.ht_test, self.terms_test, self.urlterms_test)
            else:
                terms, test_hashtags = self.read_row(i, self.ht_test, self.terms_test, pd.DataFrame())

            num_of_hashtags_in_testset+=len(test_hashtags)
            predicted_list = self.model.predict(terms, max_p)

            for j in range (max_p) :
                current_predicted_list=predicted_list[0:j]
                recall_ratio_arr[j]+=self.recall (test_hashtags, current_predicted_list)

            #print ("Result {} Truth {} Predicted {}".format (recall_result, test_hashtags, predicted_list))
        recall_ratio_arr=recall_ratio_arr/num_of_hashtags_in_testset
        return 100*recall_ratio_arr

    def print_model_information (self) :
        print ("URL SUPPORT: {}".format(self.url_support))
        print ("ALL THE TWEETS HAVE URL: {}".format(self.url_only))
        print ("TRAIN TEST RATION: {}".format(self.test_size*100))
        print ("TRAIN SIZE: {}".format(self.train_num_of_rows))
        print ("TEST SIZE: {}".format(self.test_num_of_rows))




if __name__ == "__main__":
    m = tmodel(0.1, 1)
    m.fit_the_model()
    rr = m.test_the_model(25)

    print ("recall ratio is:{}".format(rr))

