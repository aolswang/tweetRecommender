from HF_IHU_URL import *
from extractors import *
import pandas as pd
import numpy as np
import math
import statistics
import cProfile

###################################################################################
# evaluation.py file
# This file reads the rae tweets file analyzes each tweet and  creates the 4 extracted files
###################################################################################


global_id =0
global_rawTweets = "data//nonRTwithHT1.txt"


###################################################################################
# check if this tweet is an original tweet (no retweets) and that it has hashtags
# return True - if this is not a RT and has hashtags otherwise False
###################################################################################
def checkForRTandHashtags(line):
    currentline = line.split(",;,")
    if (len(currentline) < 3):
        return False
    tweet_text = currentline[2]

    if (tweet_text.startswith('RT ') or tweet_text.startswith(' RT ')):
        return False

    hashtags = extract_hashtags(tweet_text)
    if (len(hashtags) == 0):
        return False

    return True
###################################################################################
# function: cretateNonRTwithHashFile
# this is used to create the nonRTwithHT file
# return none
###################################################################################
def cretateNonRTwithHashFile ():

    temp_Counter = 0
    print("try to open file")
    writeFile = open('nonRTwithHT2.txt',"w",encoding="utf8")
    for i in range(0,74):
        temp_Counter = 0
        fname = 'output' + str(i) + '.txt'
        file = open(fname, "r", encoding="utf8")
        line = file.readline()
        while (line != '') :
            if ((temp_Counter % 1000) == 0):
                print('file - ' + str(i) + 'line - ' + str(temp_Counter))
            temp_Counter +=1
            currentpos = file.tell()
            nextline = file.readline()
            while nextline != '' and nextline.find(',;,') <= 0 :
                line += nextline
                currentpos = file.tell()
                nextline = file.readline()
            file.seek(currentpos)
            if (checkForRTandHashtags(line)) :
                writeFile.writelines(line)
            line = file.readline()
        file.close()
    writeFile.close()

###################################################################################
# fuction: parse_tweet_file
# this function reads the tweet file and creates 4 files:
# 1.hashtags of each tweet
# 2.terms of each tweet
# 3.terms extracted from the uRL of each tweet
# 4.some meta data on the tweet
# return none
###################################################################################
def parse_tweet_file (fname, maxlines=1000000, rows_to_save=50):
    ht_df = pd.DataFrame()
    terms_df = pd.DataFrame()
    url_terms_df = pd.DataFrame()
    meta_df = pd.DataFrame()

    line_counter = 0
    rows_counter = 0

    file = open(fname, "r", encoding="utf8")
    line = file.readline()
    while (line != '' and line_counter < maxlines) :
        line_counter +=1
        currentpos = file.tell()
        nextline = file.readline()
        while nextline != '' and nextline.find(',;,') <= 0 :
            line += nextline
            currentpos = file.tell()
            nextline = file.readline()
        file.seek(currentpos)

        #update the dataframes

        tweet_to_extract = line.split(",;,")
        if (len(tweet_to_extract) < 3):
            continue

        tweet_text = tweet_to_extract[2]
        try:
            hashtags, urls, terms, url_terms, retweets = parse_tweet(tweet_text, True)
            h = pd.DataFrame([hashtags])
            ht_df = pd.concat([ht_df,h], ignore_index=True)
            terms_df = pd.concat([terms_df, pd.DataFrame([terms])], ignore_index=True)
            url_terms_df = pd.concat([url_terms_df, pd.DataFrame([url_terms])], ignore_index=True)
            meta = np.array([tweet_to_extract[0],tweet_to_extract[1],retweets])
            meta_df = pd.concat([meta_df,pd.DataFrame([meta])], ignore_index=True)

            rows_counter+=1
            if ((rows_counter % 2) == 0):
                print(rows_counter)

            if ((rows_counter % rows_to_save) == 0):
                print("save to csv")
                ht_df.to_csv ("hashtags.csv")
                terms_df.to_csv ("terms.csv")
                url_terms_df.to_csv ("urlterms.csv")
                meta_df.to_csv("meta.csv")
        except Exception as error:
            print(error)
            error = None

        line = file.readline()
    file.close()
    ht_df.to_csv("hashtags.csv")
    terms_df.to_csv("terms.csv")
    url_terms_df.to_csv("urlterms.csv")
    meta_df.to_csv("meta.csv")

###################################################################################
# fuction: print_statistics
# this function prints the statistic analisys of the tweets
# return: none
###################################################################################

def print_statistics (meta_df,hashtags_df, terms_df,urlTerms_df) :
    size = len(meta_df)
    numberOfOriginalTweets = 0
    numberOfTerms = 0
    numberOfURLTerms = 0
    numberOfHashtags = 0
    numberOfNonRTTerms = 0
    numberofNonRTURLTerms = 0
    numberofNonRTHashtags = 0
    tweetsWithURL = 0
    tweetsWithHashtag = 0
    RTTweets = 0
    numberOfNonRTURLTerms = 0
    numberOfNonRTHashtags = 0
    tweetsNonRTWithURL = 0
    tweetsNonRTWithHashtag = 0
    maxHashtagsPerTweet =0

    hashtagIndex = 2
    urlIndex = 3
    termsIndex = 4
    urlTermIndex = 5
    retweetIndex = 6

    urlTerms = []
    terms = []
    hashtags = []
    hashtag_dict = {}


    for index, row in urlTerms_df.iterrows():
        temp_urlTerms = row.dropna()[1:]
        if ( len(temp_urlTerms) > 0 ):
            tweetsWithURL +=1
            numberOfURLTerms += len(temp_urlTerms)
            urlTerms += temp_urlTerms.values.flatten().tolist()

    for index, row in terms_df.iterrows():
        tempTerms = row.dropna()[1:]
        numberOfTerms += len(tempTerms)
        terms += tempTerms.values.flatten().tolist()

    overallHashtagFrequencyArray = np.zeros(50)
    urlHashtagFrequencyArray = np.zeros(50)
    nonurlHashtagFrequencyArray = np.zeros(50)

    for index, row in hashtags_df.iterrows():
        tempHashtags = row.dropna()[1:]
        tempHashtags = tempHashtags.values.flatten().tolist()
        length = len(tempHashtags)
        overallHashtagFrequencyArray[length]+=1
        if isinstance(urlTerms_df.iloc[index,1],str):
            urlHashtagFrequencyArray[length]+=1
        else:
            nonurlHashtagFrequencyArray[length]+=1
        numberOfHashtags += length
        if maxHashtagsPerTweet < length : maxHashtagsPerTweet =  length
        hashtags += tempHashtags
        for tag in tempHashtags :
            if ( tag in hashtag_dict ):
                hashtag_dict [tag] += 1
            else:
                hashtag_dict[tag] = 1

    print('overallHashtagFrequencyArray\n',overallHashtagFrequencyArray)
    print('urlHashtagFrequencyArray\n', urlHashtagFrequencyArray)
    print('nonurlHashtagFrequencyArray\n', nonurlHashtagFrequencyArray)

    print(('overall Statistics \n' +
           '\tnumber of records - {0} \n' +
           '\ttweetsWithURL - {1} \n' +
           '\tnumberOfTerms - {2} \n' +
           '\tnumberofURLTerms - {3} \n' +
           '\tnumberofHashtags - {4}').format(size-1,
                                              tweetsWithURL,
                                              numberOfTerms,
                                              numberOfURLTerms,
                                              numberOfHashtags))

    df = pd.pivot_table(meta,index=['1'],aggfunc=np.count_nonzero)

    print(('user statistics \n' +
           '\t number of users - {0} \n' +
           '\t avvrage tweets per user - {1}\n' +
           '\t max number of tweets - {2}\n' +
           '\t median number of tweets - {3}').format(len(df['0']),
                                                      df['0'].mean(),
                                                      df['0'].max(),
                                                      df['0'].median()))

    freshHashtags = sum(1 for i in hashtag_dict.values() if i == 1)
    maxHashtagAperance = max(hashtag_dict.values())
    #medianHashtagApearance = np.median(hashtag_dict.values())

    print(('hashtag statistics \n' +
           '\t numberofHashtags - {0} \n' +
           '\t number of unique hashtags - {1} \n' +
           '\t avvrage hashtag per tweet - {2}\n' +
           '\t max hashtag per tweet  - {3}\n' +
           '\t median hashtag per tweet - {4}\n' +
           '\t Average hashtag apearance - {5}\n' +
           '\t max hashtag apearance - {6}\n' +
           '\t median hashtag apearance - {7}\n' +
           '\t freshHashtags - {8}').format(numberOfHashtags,
                                            len(hashtag_dict),
                                            numberOfHashtags / size,
                                            maxHashtagsPerTweet,
                                            np.NaN,
                                            np.NaN,
                                            maxHashtagAperance,
                                            np.NaN,
                                            freshHashtags))

###################################################################################
# fuction: loadDataFrames
# loads the 4 files into dataframes
# return - the 4 dataframes
###################################################################################

def loadDataFrames():
    meta= pd.read_csv('data\\meta.csv')#,nrows=100)
    hashtags = pd.read_csv('data\\hashtags.csv')#,nrows=100)
    terms = pd.read_csv('data\\terms.csv')#,nrows=100)
    urlterms = pd.read_csv('data\\urlterms.csv')#,nrows=100)

    return meta,hashtags,terms,urlterms

###################################################################################
# fuction: loadDataFrames
#
###################################################################################

if __name__ == "__main__":

    parse_tweet_file(global_rawTweets)

    meta, hashtags, tweetTerms, urlTerms = loadDataFrames()

    print_statistics(meta, hashtags, tweetTerms, urlTerms)

    model = HF_IHU_URL()
