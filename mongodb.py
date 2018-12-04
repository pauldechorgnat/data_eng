# importing MongoDB client
from pymongo import MongoClient

# importing other packages
import pandas as pd 
from tqdm import tqdm
import gzip
import os
import wget
import shutil


if __name__ == '__main__':
    # name of the directory in which we will download the data
    directory = 'IMDB_dataset'
    zipped_file_path = os.path.join(directory, 'name.basics.tsv.gz')
    final_file_path = os.path.join(directory, 'name.basics.tsv')
    # url of the dataset to download
    download_url = 'https://datasets.imdbws.com/name.basics.tsv.gz'
    
    # if the directory does not exist we need to create one
    try:
        os.stat(directory)
    except:
        os.mkdir(directory)
    
    
    # downloading the dataset
    if 'name.basics.tsv.gz' not in os.listdir(directory):
        print('starting download ...')
        wget.download(url=download_url, out=directory)
    
    # extracting the dataset
    if 'name.basics.tsv' not in os.listdir(directory):
        print('starting extraction ...')
        with gzip.open(zipped_file_path) as input_file:
            with open(final_file_path, 'wb') as output_file:
                shutil.copyfileobj(input_file, output_file)
    
    # creating a client to connect to MongoDB
    client = MongoClient()

    # collecting the database names
    # printing this is the equivalent of "show dbs;"
    database_names = client.list_database_names()
    print('databases')
    print(database_names)
    print()

    # creating or using a database 
    # equivalent to use IMDB;
    db = client.IMDB

    # collecting the names of the collections of IMDB
    collection_names = db.list_collection_names()
    print('collections of {}'.format(db.name))
    print(collection_names)

    # creating a collection actors
    actor_collection = db.actors
    
    input('press enter to continue')

    # path to the file containing data of actors 
    path_to_file = os.path.join(directory, 'name.basics.tsv')
    
    # reading the first 10 lines of our file 
    df = pd.read_csv(path_to_file, sep='\t', nrows=10)
    print(df)
    
    input('press enter to continue')
    print()
    print('parsing flat file...')
    print('--- you can interrupt this thread at any time by pressing ctrl+C')
    
    # opening the flat file containing information about actors, realisators, ...
    with open(path_to_file, 'r') as file:
        # defining a counter for the number of lines
        counter = 0
        # allowing user interruption
        try:
            # running through the file
            for line in tqdm(file):

                # skipping the first line
                if counter > 0:
                    # parsing the line
                    fields = str(line).replace('\n', '').split('\t')
                        
                    id_= fields[0]
                    name = fields[1]
                    # birth and death date are not always known
                    # so we need to enclose those statement into try/except blocks
                    try: 
                        birth = int(fields[2])
                    except ValueError:
                        birth = None

                    try: 
                        death = int(fields[3])
                    except ValueError:
                        death=None
                    
                    # parsing professions 
                    professions = fields[4].split(',')
                    # parsing known movies 
                    knownMovies = fields[5].split(',')
                    
                    # creating a dictionnary to insert in the database
                    obj = {}
                    # creating the key-value for all the attributes
                    obj['id'] = id_
                    obj['name'] = name
                    if birth is not None: obj['birth'] = birth
                    if death is not None: obj['death'] = death
                    # creating a key-value pair for each profession
                    for i in range(len(professions)):
                        obj['profession_{}'.format(i+1)] = professions[i]
                    # feeding an array to the known movies attribute
                    obj['knownMovies'] = knownMovies
                    # inserting objects into the database
                    actor_collection.insert(obj)
                # incrementing counter
                counter +=1
        # allowing user interruption
        except KeyboardInterrupt:
            print('user interruption')
            
    print('{} objects inserted'.format(counter))
            
    client.close()
            
