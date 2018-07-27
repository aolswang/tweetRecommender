#######################################################################################
#The tweet.py file is in charge of reading live tweets and dumping it to a file
#######################################################################################


import twython
import numpy as np
from time import sleep, time
import json
import codecs

CONSUMER_KEY = "7ltX8F0vUn30aHWytulVoot5S"
CONSUMER_SECRET = "cmTfSV81rwp6jWBoWOAHCyWoa95AYg22OYfSMrcuoWW8a4LF2d"
OAUTH_TOKEN = "90821027-4ET1pv1QdYS97jE9kXe2hz8zvlMclZcHgXqW4cg84"
OAUTH_TOKEN_SECRET = "qYu4urGMBywUNrOKnVCX1sTRozP7HlIQn3owKwx6g0a1a"

counter = 0
text_file = codecs.open("Output0.txt", "w" , "utf-8")
fileIndex = 1
#create files of 100K tweets per file
FileSize = 100000


#######################################################################################
#this is tweeter streaming callback class
#the callback dumps the tweet to a file and rotates the file every 100K tweets
#######################################################################################

class MyStreamer(twython.TwythonStreamer):

    def on_success(self, data):
        global counter
        global fileIndex
        global FileSize
        global text_file

        if 'text' in data:

            username = data["user"]["screen_name"]


            message = "{0} ,;, {1} ,;, {2} \n".format(data['created_at'], username, data['text'])
            text_file.write(message)


            print(str(counter) + " - " + data['created_at'] + ' - ' + username + ' - ' + data['text'])
            counter += 1

            if counter == FileSize:
                counter = 0
                text_file.close()
                filename = "Output" + str(fileIndex) + '.txt'
                fileIndex += 1
                text_file = codecs.open(filename, "w" , "utf-8")

    def on_error(self, status_code, data):
        print(status_code)

#######################################################################################
#this is the main
#create streaming instance if the connection is lost ctahc the exception and try again
#######################################################################################

for i in range(0,1000000) :
    try :
        stream = MyStreamer(CONSUMER_KEY, CONSUMER_SECRET,OAUTH_TOKEN,OAUTH_TOKEN_SECRET)
        stream.statuses.filter(track = 'a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z')
    except Exception as e:
        sleep(10)
        print('error - ' + str(i))
        print(e)

