import tweepy
import pandas as pd
import re
import urlexpander as uex
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
# from pprint import pprint
# auth = tweepy.OAuthHandler(os.getenv('API_KEY'), os.getenv('API_KEY_SECRET'))

API_KEY="OGB1FukcGYIAGZGagKoUBCibT"
API_KEY_SECRET="lAX1ZqOE2XafCWupaqGAJlRRdSTME3kLh8F5NVOFsNOgXe1ei6"
ACCESS_TOKEN="1334718327860514818-NDpvYLmr8qE2lIL11IxX6EEO9Ecn0U"
ACCESS_TOKEN_SECRET="4GhWKZioSSBTIDejkAXKfbbneL0HtWnu4TbhntPKQb3eF"
auth = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

minute_count = 1

status_list = []

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        global status_list, minute_count
        status_list.append([status.text, status.user.screen_name, self.keyword, minute_count])

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False
    




def start_stream(keyword_list):
    my_stream_listener = MyStreamListener()
    my_stream_listener.keyword = keyword_list[0]
    my_stream = tweepy.Stream(auth=auth, listener=my_stream_listener)
    my_stream.filter(track=keyword_list, is_async=True, stall_warnings=True)

def user_report(df):
    # print("Generating user report")
    df['Count'] = 1
    user_report_df = df.groupby(["Username", "Keyword"]).count()["Count"]
    print(user_report_df.to_string())

def link_report(df):
    tweet_df = df['Tweet']
    all_tweet_links = ""
    for tweet in tweet_df:
        all_tweet_links += tweet + " "

    all_urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', all_tweet_links)

    print("*"*50)
    print("Total number of links : " + str(len(all_urls)))
    print("*"*50)

    links = {
        'Links' : all_urls
    }

    link_df = pd.DataFrame(links, columns=['Links'])

    
    domain_count = {}
    for link in link_df['Links']:
        url = uex.expand(link)
        domain = uex.get_domain(url)
        # print(domain)
        if domain in domain_count:
            domain_count[domain] += 1
        else:
            domain_count[domain] = 1
    
    # print(domain_count)
    
    domain_df = pd.DataFrame.from_dict(domain_count, orient='index', columns=[ 'Count'])

    print(domain_df.sort_values(by=['Count'], ascending=False).to_string())

def content_report(df):
    tweet_df = df['Tweet']
    all_tweets = ""
    for tweet in tweet_df:
        all_tweets += tweet + " "

    other_stop_words = ['@',',','#','rt', '.', '!', ':', '?', ';', 'https', '$', '%', '&', '\`', "'", "''","...","-","`","``","'s","’",'(',"|"]
    stop_words = stopwords.words('english') + other_stop_words
    word_tokens = word_tokenize(all_tweets.lower())
    filtered_sentence = [w for w in word_tokens if not w in stop_words]

    context_df = pd.DataFrame({'words' : filtered_sentence})

    context_df['Count'] = 1
    print("*"*50)
    print("Number of unique words "+ str(context_df.size))
    print("*"*50)
    print("*"*50)
    print("Top 10 words sorted by occurence")
    print("*"*50)
    context_df = context_df.groupby(['words']).count().sort_values(['Count'], ascending=False)[:10]

    print(context_df.to_string())

def generate_report(df):
    print("\n")
    print("*"*50)
    print("*"*20 + "User Report" + "*"*20)
    print("*"*50)
    user_report(df)
    print("\n")
    print("*"*50)
    print("*"*20 + "Link Report" + "*"*20)
    print("*"*50)
    link_report(df)
    print("\n")
    print("*"*50)    
    print("*"*20 + "Content Report" + "*"*20)
    print("*"*50)
    content_report(df)
    global minute_count
    minute_count += 1

def generate_report_task_1():
    global status_list
    df = pd.DataFrame(status_list, columns=['Tweet', 'Username', 'Keyword', 'Minute'])
    status_list = []
    generate_report(df)

def generate_report_task_2():
    global status_list, minute_count    
    df = pd.DataFrame(status_list, columns=['Tweet', 'Username', 'Keyword','Minute'])
    if minute_count > 5:
        start_minute = minute_count - 5
        df = df.loc[df['Minute'] >= start_minute]
        
    generate_report(df)


