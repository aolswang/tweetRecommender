from HF_IHU_URL import *
import pandas as pd
from sklearn.model_selection import train_test_split

class tmodel:

    def __init__(self, test_size=0.1, seed=1) :
        self.model = HF_IHU_URL()

        ht = pd.read_csv("hashtags.csv")
        terms = pd.read_csv("terms.csv")
        urlterms = pd.read_csv("urlterms.csv")

        self.ht_train, self.ht_test, self.terms_train, self.terms_test, self.urlterms_train, self.urlterms_test = train_test_split(ht, terms,
                                                                                                     urlterms,
                                                                                                     test_size=test_size,
                                                                                                     random_state=seed)
        self.train_num_of_rows = self.ht_train.shape[0]
        self.test_num_of_rows = self.ht_test.shape[0]

    def read_row (self, k, ht_df, terms_df, urlterms_df) :
        hashtags = ht_df.iloc[k:k+1, 1:].dropna(axis=1)
        hashtags = hashtags.values.flatten().tolist()

        t = terms_df.iloc[k:k+1, 1:].dropna(axis=1)
        urlt = urlterms_df.iloc[k:k+1, 1:].dropna(axis=1)
        terms = pd.concat([t, urlt], axis=1, ignore_index=True)
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

    def test_the_model (self, p) :
        recall_ratio = 0
        for i in range(self.test_num_of_rows):
            terms, test_hashtags = self.read_row(i, self.ht_test, self.terms_test, self.urlterms_test)
            predicted_list = self.model.predict(terms, p)
            recall_result = self.recall (test_hashtags, predicted_list )
            recall_ratio+=recall_result
            print ("Result {} Truth {} Predicted {}".format (recall_result, test_hashtags, predicted_list))
        recall_ratio=recall_ratio/self.test_num_of_rows
        return recall_ratio


if __name__ == "__main__":
    m = tmodel(0.1, 1)
    m.fit_the_model()
    rr = m.test_the_model(3)
    print ("recall ratio is:{}".format(rr))

