import string
import re
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
import requests

def extract_hashtags (text) :
   return set(part[1:] for part in text.split() if part.startswith('#'))

def extract_urls (text):
    urls = re.findall(r'(https?://\S+)', text)
    return urls

def remove_urls (text) :
    return re.sub(r'(https?://\S+)', '', text)

def remove_hashtags_and_mentions (text) :
    no_hshtags = re.sub(r'(#\S+)', '', text)
    return re.sub(r'(@\S+)', '', no_hshtags)


def split_to_words (text) :
    return  word_tokenize(text)


def remove_punctuation (words):
    table = str.maketrans('', '', string.punctuation)
    stripped = [w.translate(table) for w in words]
    # remove remaining tokens that are not alphabetic
    words = [word for word in stripped if word.isalpha()]
    return words

def normalizing_case (words) :
    words = [word.lower() for word in words]
    return words

def filter_out_stop_words(words):
    # filter out stop words
    stop_words = set(stopwords.words('english'))
    words = [w for w in words if not w in stop_words]
    return words

def stem_words (words) :
    # stemming of words
    porter = PorterStemmer()
    stemmed = [porter.stem(word) for word in words]
    return stemmed

def remove_duplicate (words):
    return set (words)

def extract_text_cleaned (text) :
    text = remove_hashtags_and_mentions(text)
    text = remove_urls(text)
    words = split_to_words(text)
    words = remove_punctuation(words)
    words = normalizing_case(words)
    words = filter_out_stop_words(words)
    words = stem_words(words)
    #words = remove_duplicate(words)
    return words

def extract_url_title (url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.title.string
        words = extract_text_cleaned (title)
        return words
    except Exception as e:
        return []

def parse_tweet (text, parse_url=False) :
    hashtags = extract_hashtags (text)
    urls = extract_urls(text)

    tweet_terms = extract_text_cleaned (text)
    url_terms = []
    if (parse_url) :
        url_terms = list(map(extract_url_title, urls))
        flat_list = [item for sublist in url_terms for item in sublist]
        url_terms = flat_list

    return hashtags, urls, tweet_terms, url_terms



