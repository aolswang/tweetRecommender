from HF_IHU_URL import *
from extractors import *
import pandas as pd


def cretate_data_frame (fname) :
    columns = ['id', 'date', 'user', 'hashtags', 'urls', 'terms', 'url_terms']
    df = pd.DataFrame([],columns=columns)

    with open(fname, "r", encoding="utf8") as filestream:
        for line in filestream:
            tmp_df = pd.DataFrame([], columns=columns)
            currentline = line.split("-")
            if (len(currentline)<4):
                continue
            tmp_df.at [0, 'id'] = currentline[0]
            tmp_df.at[0, 'date'] = currentline[1]
            tmp_df.at[0, 'user'] = currentline[2]
            tweet_text = ''.join(currentline[3:])

            tmp_df.at[0, 'hashtags'], tmp_df.at [0, 'urls'], tmp_df.at [0, 'terms'], tmp_df.at [0, 'url_terms'] = parse_tweet(tweet_text, False)
            #tmp_df = pd.DataFrame([id, date, user, hashtags, urls, terms, url_terms], columns=columns)
            df= df.append(tmp_df, ignore_index=True)

    return df

if __name__ == "__main__":

    model = HF_IHU_URL()

    data = cretate_data_frame ("data\sampleTweets.txt")
    print (data.head())
    tweet_example = "#helpeve ASK QUESTIONS. GET ANSWERS. give a little, get a little. Its free. http://buff.ly/1d2rjmm (@projecteve)"
    print(tweet_example)
    hashtags, urls, terms, url_terms = parse_tweet(tweet_example + tweet_example, True)
    model.fit (terms, hashtags)
    h = model.predict(terms, 6)
    print ("hi")
