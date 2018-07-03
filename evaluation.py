from HF_IHU_URL import *
from extractors import *
import pandas as pd
import numpy as np
import cProfile

global_id =0





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

# this is used to create the nonRTwithHT file
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


def parse_tweet_file (fname, maxlines=1000, rows_to_save=100):
    user_df = pd.DataFrame()
    ht_df = pd.DataFrame()
    terms_df = pd.DataFrame()
    url_terms_df = pd.DataFrame()

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

            rows_counter+=1
            if ((rows_counter % 2) == 0):
                print(rows_counter)

            if ((rows_counter % rows_to_save) == 0):
                print("save to csv")
                ht_df.to_csv ("hashtags.csv")
                terms_df.to_csv ("terms.csv")
                url_terms_df.to_csv ("urlterms.csv")
        except Exception as error:
            error = None

        line = file.readline()
    file.close()
    ht_df.to_csv("hashtags.csv")
    terms_df.to_csv("terms.csv")
    url_terms_df.to_csv("urlterms.csv")


def print_data_np_statistics (data) :
    size = len(data)
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

    hashtagIndex = 2
    urlIndex = 3
    termsIndex = 4
    urlTermIndex = 5
    retweetIndex = 6

    columns = ['date', 'user', 'hashtags', 'urls', 'terms', 'url_terms','retweet']

    for i in range(1,size) :
        if data[i,termsIndex] != None : numberOfTerms += len(data[i,termsIndex])
        if data[i, urlTermIndex] != None: numberOfURLTerms += len(data[i, urlTermIndex])
        if data[i, hashtagIndex] != None: numberOfHashtags += len(data[i, hashtagIndex])
        if data[i, urlIndex] != None: tweetsWithURL += 1
        if data[i, hashtagIndex] != None: tweetsWithHashtag += 1
        if data[i, retweetIndex] == True: RTTweets += 1

        if (data[i, retweetIndex] == False):
            numberOfOriginalTweets +=1
            if data[i,termsIndex] != None : numberOfNonRTTerms += len(data[i,termsIndex])
            if data[i, urlTermIndex] != None: numberOfNonRTURLTerms += len(data[i, urlTermIndex])
            if data[i, hashtagIndex] != None: numberOfNonRTHashtags += len(data[i, hashtagIndex])
            if data[i, urlIndex] != None: tweetsNonRTWithURL += 1
            if data[i, hashtagIndex] != None: tweetsNonRTWithHashtag += 1


    print(('overall Statistics \n' +
           '\tnumber of records - {0} \n' +
           '\ttweetsWithURL - {1} \n' +
           '\ttweetsWithHashtag - {2} \n' +
           '\tRTTweets - {3} \n' +
           '\tnumberOfTerms - {4} \n' +
           '\tnumberofURLTerms - {5} \n' +
           '\tnumberofHashtags - {6}').format(size-1,tweetsWithURL,tweetsWithHashtag,RTTweets,numberOfTerms,numberOfURLTerms,numberOfHashtags))


    print(('Statistics for original tweets (non RT) \n' +
           '\tnumber of records - {0} \n' +
           '\ttweetsWithURL - {1} \n' +
           '\ttweetsWithHashtag - {2} \n' +
           '\tnumberOfTerms - {3} \n' +
           '\tnumberofURLTerms - {4} \n' +
           '\tnumberofHashtags - {5}').format(numberOfOriginalTweets,tweetsNonRTWithURL,tweetsNonRTWithHashtag,numberOfNonRTTerms,numberofNonRTURLTerms,numberofNonRTHashtags))


def buildNPData() :

    myNP = cretate_numpy()
    print_data_np_statistics(myNP)
    '''
    #trying to dump np to file

    np.save('np.csv',myNP)
    with open('np.csv') as f:
        foo = f.readlines()
    myNP = np.load(foo)
    print_data_np_statistics(myNP)
    '''

if __name__ == "__main__":
    parse_tweet_file("data/nonRTwithHT1.txt")
    model = HF_IHU_URL()
