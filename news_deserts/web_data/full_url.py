import pandas as pd
import glob
import re
import math
import urllib.request
from bs4 import BeautifulSoup
from operator import itemgetter
import csv
import os
from textblob import TextBlob
import matplotlib.pyplot as plt
import numpy as np
import requests
from urllib.parse import urlparse

names = ["TweetID", "Timestamp", "Full_Text", "In_Reply_To_User_ID", "User_ID", "User_Name", "User_Screen_Name",
         "Coordinates", "Place", "Bounding_Box", "Quoted_Status_ID", "Retweeted_Status", "Hashtags", "URLs", "User_Mentions",
         "Media", "Language"]


def news_deserts_csv_df(filename):

    data_basepath = "/Users/tdt62/Desktop/GraduateResearch/test_data/"

    print(filename, flush=True)
    try:
        df = pd.read_csv(data_basepath + filename, index_col=None, header=None, sep='\t', names=names)

    except pd.errors.ParserError:
        df = pd.read_csv(data_basepath + filename, index_col=None, header=None, names=names)

    except:
        print("The file {} was not read in".format(filename))
    # Takes all of the csv file and makes one big dataframe
    # Makes the big df in memory
    df.fillna("NA", inplace=True)

    return df

def get_top_domains(df, top_val):
    top_list = []
    domain_dict = {}

    def iterate_domains(df):
#         filtered_urls = df[(df['URLs'] != '[]')]
        top_list = []
        domain_dict = {}
        for url in df:
            try:
                response = requests.get(url.strip("''[]"))
                new_url = urlparse(response.url)
                if new_url.netloc in domain_dict:
                    domain_dict[new_url.netloc] += 1
                else:
                    domain_dict[new_url.netloc] = 1
            except:
                new_url = urlparse(url)
                if new_url.netloc in domain_dict:
                    domain_dict[new_url.netloc] += 1
                else:
                    domain_dict[new_url.netloc] = 1

    # Filter the df

    filtered_df = df[(df['URLs'] != '[]')]

    list(map(lambda x: iterate_domains(x), df[(df['URLs'] != '[]')]['URLs']))

    return domain_dict


def make_csv(top_dict, filename, filepath, headers=["Domain", "Count"]):
    with open(filename + filepath, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)
        for key, value in top_dict.items():
            writer.writerow([key, value])


if __name__== "__main__":

    # # Create big df for manipulating the data
    news_df = news_deserts_csv_df("2018_10_17_16_stream_1_clean.csv", )
    filtered_urls = news_df[(news_df['URLs'] != '[]')]

    # Gets top 20 domains
    domain_dict = get_top_domains(filtered_urls, 20)
    make_csv(domain_dict, "/Users/tdt62/Desktop/", "top_domains_unshort.csv", headers=['Domain', 'Count'])
