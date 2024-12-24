import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import re
import boto3
import base64
from io import StringIO


object_key = "scrape_marvel_movie_data/"

# Step 1: Scrape movie titles from Wikipedia
def scrape_marvel_movies():
    wiki_url = "https://en.wikipedia.org/wiki/List_of_Marvel_Cinematic_Universe_films"
    response = requests.get(wiki_url)
    var = BeautifulSoup(response.content, 'html.parser')
    table = var.find_all('table', class_='wikitable')
    column = []
    for tbl in table[:1]:
        header_row = tbl.find_all('th', scope='col')
        for col in header_row:
            column.append(col.get_text().strip().replace('\n', ''))
    wiki_data = []
    film_names = []
    for tbl in table[:7]:
        rows = tbl.find_all('th', scope='row')
        for row in rows:
            film_names.append(row.get_text().replace('\n', ''))
        for row in rows:
            data = row.find_next_siblings('td')
            text = [i.get_text().replace('\n', '').replace('\xa0', ' ') for i in data]
            text = [re.sub(r'\(.*?\)', '', t) for t in text]  # Remove hidden date part
            wiki_data.append(text)

    # Append film names to the corresponding wiki_data sublists
    for i, film_name in enumerate(film_names):
        wiki_data[i].insert(0, film_name)

    # Ensure each list in wiki_data has exactly 5 elements
    for row in wiki_data:
        while len(row) < len(column):
            row.append(None)  # Append None for missing values
        while len(row) > len(column):
            row.pop()  # Remove extra elements

    # Create DataFrame with the correct shape
    wiki_data_df = pd.DataFrame(wiki_data, columns=column)
    return wiki_data_df

# Step 2: Clean the scraped movie data
def clean_movie_data(movies_df):
    # Define new colum names
    new_column_names = {
        'Film[30]': 'film_name',
        'U.S. release date': 'us_release_date',
        'Director': 'director_name',
        'Screenwriter(s)': 'screen_writer',
        'Producer(s)': 'producers'
    }

    # Rename columns
    movies_df.rename(columns=new_column_names, inplace=True)

    # Forward fill missing screen_writer values
    movies_df['screen_writer'] = movies_df['screen_writer'].ffill()

    # Forward fill missing producers values
    movies_df['producers'] = movies_df['producers'].ffill()

    # Remove numbers in square brackets from all columns
    movies_df.replace(to_replace=r'\[\d+\]', value='', regex=True, inplace=True)


    return movies_df


# Step 3: Fetch additional movie data from OMDB API
def fetch_omdb_data(movies_df_cleaned):
    # Access the environment variables
    api_key = os.getenv('API_KEY')
    response_list = []
    for film_name in movies_df_cleaned['film_name']:
        url = f'http://www.omdbapi.com/?t={film_name}&apikey={api_key}'
        response = requests.get(url)
        response_list.append(response.json())

    return response_list


# Step 4: Save the cleaned data to a DataFrame
def clean_omdb_data(omdb_data_df):

    # Remove keys and values with 'Movie not found!' from dictionary
    for i in omdb_data_df:
        if i.get('Error') == 'Movie not found!':
            i.pop('Error', None)

    omdb_data_df = pd.DataFrame(omdb_data_df)

    # Select only the columns you want to keep
    columns_to_keep = ['Title', 'Rated', 'Released', 'Runtime', 'Genre', 'Actors', 'Awards', 'imdbRating', 'BoxOffice', 'Type']
    omdb_data_df = omdb_data_df[columns_to_keep]

    new_column_names = {
        'Title': 'film_name',
        'Rated': 'rated',
        'Released': 'release_date',
        'Runtime': 'runtime',
        'Genre': 'genre',
        'Actors': 'actors',
        'Awards': 'awards',
        'imdbRating': 'imdb_rating',
        'BoxOffice': 'box_office_collection',
        'Type': 'type',
    }
    # Rename columns to merge with wiki_data_df
    omdb_data_df.rename(columns=new_column_names, inplace=True)

    # Remove rows with null values in the film_name column
    omdb_data_df.dropna(subset=['film_name'], inplace=True)

    return omdb_data_df

# Step 5: Merge the two DataFrames
def merged_data(movies_df_cleaned, omdb_data_df_cleaned):
    # Joining the two dataframes to get the final result
    final_df = movies_df_cleaned.merge(omdb_data_df_cleaned, on='film_name', how='left')
    final_df.fillna('N/A', inplace=True)
    # final_df.to_csv('final_df.csv', index=False)
    return final_df


# Step 6: Upload DataFrames to S3 with specific folder paths
def upload_to_s3(df, object_key, file_name):
    # Access the environment variables
    s3_bucket_name = os.getenv('S3_BUCKET_NAME')
    s3 = boto3.client('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=s3_bucket_name, Key=f"{object_key}{file_name}", Body=csv_buffer.getvalue())


def lambda_handler(event, context):
    # Step 1: Scrape movie titles from Wikipedia
    movies_df = scrape_marvel_movies()

    # Step 2: Clean the scraped movie data
    movies_df_cleaned = clean_movie_data(movies_df)

    # Step 3: Fetch additional movie data from OMDB API
    omdb_data_list = fetch_omdb_data(movies_df_cleaned)

    # Step 4: Save the cleaned data to a DataFrame
    omdb_data_df_cleaned = clean_omdb_data(omdb_data_list)

    # Step 5: Merge the two DataFrames
    merged_df = merged_data(movies_df_cleaned, omdb_data_df_cleaned)

    # Upload DataFrames to S3 with specified folder paths
    upload_to_s3(movies_df_cleaned, object_key, 'movies.csv')
    upload_to_s3(omdb_data_df_cleaned, object_key, 'omdb_data_df_cleaned.csv')
    upload_to_s3(merged_df, object_key, 'merged_df.csv')
    
    return {
        'statusCode': 200,
        'body': 'Data uploaded successfully'
}
