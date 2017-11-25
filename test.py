import pandas as pd
import numpy as np
import matplotlib as plt
from py2neo import Graph

graph = Graph('http://neo4j:melon123!@localhost:7474/db/data/')

# Loading user-related data
user = pd.read_csv('ml-100k/u.user', sep='|', header=None, names=['id','age','gender','occupation','zip code'])
n_u = user.shape[0]

# Loading genres of movies
genre = pd.read_csv('ml-100k/u.genre', sep='|', header=None, names=['name', 'id'])
n_g = genre.shape[0]

# Loading item-related data
# Format : id | title | release date | | IMDb url | "genres"
# where "genres" is a vector of size n_g : genres[i]=1 if the movie belongs to genre i
movie_col = ['id', 'title','release date', 'useless', 'IMDb url']
movie_col = movie_col + genre['id'].tolist()
movie = pd.read_csv('ml-100k/u.item', sep='|', header=None, names=movie_col)
movie = movie.fillna('unknown')
n_m = movie.shape[0]

# Loading ratings
rating_col = ['user_id', 'item_id','rating', 'timestamp']
rating = pd.read_csv('ml-100k/u.data', sep='\t' ,header=None, names=rating_col)
n_r = rating.shape[0]