import csv
import os
import pandas as pd
import argparse

names = ["TweetID", "Timestamp", "Full_Text", "In_Reply_To_User_ID", "User_ID", "User_Name", "User_Screen_Name",
         "Coordinates", "Place", "Bounding_Box", "Quoted_Status_ID", "Retweeted_Status", "Hashtags", "URLs", "User_Mentions",
         "Media", "Language"]



def news_deserts_csv_df(keywords, filename, filepath, political=True):

    data_basepath = "/projects/canis/news_deserts/twitter/data/"
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

    def check_keywords(x):
        for keyword in keywords:
            if(keyword.lower() in x.lower()):
                return True
        return False

    # Make a seperate column which is True if it has the keyword, False if not
    df['has_keyword'] = list(map(lambda x: check_keywords(x), df['Full_Text']))

    # Sort based on True/False values from above
    keywords_df = df.loc[df['has_keyword'] == True]

    if(political):
        ending = "_political"
    else:
        ending = "_place"

    keywords_df.to_csv(filepath + filename[:-4] + ending + ".csv")

    return keywords_df

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('csv_file', action='store', help='Provide the file you wish to run.')
    args = parser.parse_args()
    return args

if __name__== "__main__":

    command_line_args = parse_arguments()

    political_keywords = ["Midterm", "Vote", "Politics", "District", "Senator", "Congress", "elect","Representative",
                        "Sen","Rep","Republican","Democrat","Dem","Rep","Gov","Debates","Poli","GOP","Ballot","Register",
                        "Incumbent","Delegate","Potus","Scotus","Supreme court"]

    political_basepath = "/projects/canis/scripts/graduate_research/news_deserts_stats/split_data/political_keywords/"
    political_news_df = news_deserts_csv_df(political_keywords, command_line_args.csv_file, political_basepath)


    place_keywords = ["GA","KS","TX","NE","KY","MO","MS","CA","TN","FL","SD","OK","Georgia","Kansas","Texas",
                    "Nebraska","Kentucky","Missouri","Mississippi","California","Tennessee","Florida",
                    "South Dakota","Oklahoma"]

    place_basepath = "/projects/canis/scripts/graduate_research/news_deserts_stats/split_data/place_keywords/"
    places_news_df = news_deserts_csv_df(place_keywords, command_line_args.csv_file, place_basepath, political=False)
