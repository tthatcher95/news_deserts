import csv
import os
import pandas as pd
import argparse

def news_deserts_csv_df():

    names = ["TweetID", "Timestamp", "Full_Text", "In_Reply_To_User_ID", "User_ID", "User_Name", "User_Screen_Name",
             "Coordinates", "Place", "Bounding_Box", "Quoted_Status_ID", "Retweeted_Status", "Hashtags", "URLs", "User_Mentions",
             "Media", "Language"]

    # Last date that is "clean"
    basepath = "/projects/canis/scripts/graduate_research/news_deserts_stats/split_data/place_keywords/"
    # basepath = "/projects/canis/scripts/graduate_research/news_deserts_stats/split_data/political_keywords"

    political_file = "political.txt"
    place_file = "place.txt"

    # filename = "/projects/canis/scripts/graduate_research/news_deserts_stats/split_data/" + political_file
    filename = "/projects/canis/scripts/graduate_research/news_deserts_stats/split_data/" + place_file

    list_ = []

    # Takes all of the csv file and makes one big dataframe
    datafile = open(filename, 'r')
    filelist = [basepath + line.strip('\n') for line in datafile.readlines()]
    reject_list = []

    for name in filelist:
        try:
            df = pd.read_csv(data_basepath + filename, index_col=None, header=None, sep='\t', names=names)

        except pd.errors.ParserError:
            df = pd.read_csv(data_basepath + filename, index_col=None, header=None, names=names)

        except Exception as e:
            print(e)
            reject_list.append(name)

    # Takes all of the csv file and makes one big dataframe
    # Makes the big df in memory
    frame = pd.concat(list_, axis = 0, ignore_index = True)
    frame.fillna("NA", inplace=True)

    with open('rejected_files.txt', 'w') as f:
        for element in reject_list:
            f.write("{}\n".format(str(element)))
        f.close()

    return frame


def hashtags(df, top_val):

    top_list = []
    hashtag_dict = {}

    def iterate_hashtags(x):
        hashtag_list = list(x.split("'text':"))
        for element in hashtag_list:
            stripped_element = element.split(',')[0].strip("' '{}[]")
            if stripped_element in hashtag_dict and stripped_element != '[]' and stripped_element != 'NA' and stripped_element != '':
                hashtag_dict[stripped_element] += 1
            else:
                hashtag_dict[stripped_element] = 1

    # Create the hashtag dict
    list(map(lambda x: iterate_hashtags(x), df['Hashtags']))

    return hashtag_dict

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

def top_user_mentions(df, top_val):

    top_users_list = []
    hashtag_dict = {}

    def iterate_user_mentions(x):

        hashtag_list = list(x.split("'screen_name':"))
        for element in hashtag_list:
            stripped_element = element.split(',')[0].strip("' '{}[]")
            if stripped_element in hashtag_dict and stripped_element != '[]' and stripped_element != 'NA' and stripped_element != '':
                hashtag_dict[stripped_element] += 1
            else:
                hashtag_dict[stripped_element] = 1

    # Create the hashtag dict
    list(map(lambda x: iterate_user_mentions(x), df['User_Mentions']))

    return hashtag_dict

def get_top_noun_phrases(df, top_val):
    top_list = []
    phrase_dict = {}

    def iterate_phrases(x):
        blob = TextBlob(x)
        for element in blob.noun_phrases:
            if element in phrase_dict and element != "NA":
                phrase_dict[element] += 1
            else:
                phrase_dict[element] = 1

    # Create the hashtag dict
    list(map(lambda x: iterate_phrases(x), df['Full_Text']))

    return phrase_dict


if __name__== "__main__":

    # # Create big df for manipulating the data
    news_df = news_deserts_csv_df()

    output_dir = "/projects/canis/scripts/graduate_research/news_deserts_stats/split_data/keyword_stats/places/"
    # output_dir = "/projects/canis/scripts/graduate_research/news_deserts_stats/split_data/keyword_stats/political/"

    domain_dict = get_top_domains(news_df, 20)
    make_csv(domain_dict, output_dir, "top_place_domain.csv", headers=['Domain', 'Count'])
    # make_csv(domain_dict, output_dir, "top_political_domain.csv", headers=['Domain', 'Count'])



    # Gets the list for top 20 hashtags
    hash_dict = hashtags(news_df, 20)
    make_csv(hash_dict, output_dir, "top_place_hashtags.csv", headers=['Hashtag', 'Count'])
    # make_csv(domain_dict, output_dir, "top_political_hashtag.csv", headers=['Hashtag', 'Count'])


    # Get the top 20 user mentions
    phrase_dict = top_user_mentions(news_df, 20)
    make_csv(phrase_dict, output_dir, "top_place_userMentions.csv", headers=['UserMention', 'Count'])
    # make_csv(domain_dict, output_dir, "top_political_userMentions.csv", headers=['UserMention', 'Count'])
