# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import matplotlib as plt
from py2neo import Graph

graph = Graph('http://neo4j:melon123!@localhost:7474/db/data/')

# 사용자 데이터 로드
user = pd.read_csv('ml-100k/u.user', sep='|', header=None, names=['id','age','gender','occupation','zip code'])
n_u = user.shape[0]

# 장르 데이터 로드
genre = pd.read_csv('ml-100k/u.genre', sep='|', header=None, names=['name', 'id'])
n_g = genre.shape[0]


# 아이템 데이터 로드
# Format : id | title | release date | | IMDb url | "genres"
# where "genres" is a vector of size n_g : genres[i]=1 if the movie belongs to genre i
movie_col = ['id', 'title','release date', 'useless', 'IMDb url']
movie_col = movie_col + genre['id'].tolist()
movie = pd.read_csv('ml-100k/u.item', sep='|', header=None, names=movie_col)
movie = movie.fillna('unknown')
n_m = movie.shape[0]

# 평점 로드
rating_col = ['user_id', 'item_id','rating', 'timestamp']
rating = pd.read_csv('ml-100k/u.data', sep='\t' ,header=None, names=rating_col)
n_r = rating.shape[0]

# 사용자 노드 생성
##### Create the nodes relative to Users, each one being identified by its user_id #####
# "MERGE" request : creates a new node if it does not exist already
tx = graph.cypher.begin()
statement = "MERGE (a:`User`{user_id:{A}}) RETURN a"
for u in user['id']:
    tx.append(statement, {"A": u})

tx.commit()

# 장르 노드 생성
##### Create the nodes relative to Genres, each one being identified by its genre_id, and with the property name #####
tx = graph.cypher.begin()
statement = "MERGE (a:`Genre`{genre_id:{A}, name:{B}}) RETURN a"
for g,row in genre.iterrows() :
    tx.append(statement, {"A": row.iloc[1], "B": row.iloc[0]})

tx.commit()

# 아이템 노드 생성
##### Create the Movie nodes with properties movie_id, title and url ; then create the Is_genre edges #####
tx = graph.cypher.begin()
statement1 = "MERGE (a:`Movie`{movie_id:{A}, title:{B}, url:{C}}) RETURN a"
statement2 = ("MATCH (t:`Genre`{genre_id:{D}}) "
              "MATCH (a:`Movie`{movie_id:{A}, title:{B}, url:{C}}) MERGE (a)-[r:`Is_genre`]->(t) RETURN r")

# 영화 마다 루프 순회
# Looping over movies m
for m,row in movie.iterrows() :
    # Create "Movie" node
    tx.append(statement1, {"A": row.loc['id'], "B": row.loc['title'].decode('latin-1'), "C": row.loc['IMDb url']})
    # is_genre : vector of size n_g, is_genre[i]=True if Movie m belongs to Genre i
    is_genre = row.iloc[-19:]==1
    related_genres = genre[is_genre].axes[0].values

    # Looping over Genres g which satisfy the condition : is_genre[i]=True
    for g in related_genres :
        # Retrieve node corresponding to genre g, and create relation between g and m
        tx.append(statement2,\
                  {"A": row.loc['id'], "B": row.loc['title'].decode('latin-1'), "C": row.loc['IMDb url'], "D": g})

    # Every 100 movies, push queued statements to the server for execution to avoid one massive "commit"
    if m%100==0 : tx.process()

# End with a "commit"
tx.commit()

# Has_rated edge 생성
##### Create the Has_rated edges, with rating as property #####
tx = graph.cypher.begin()
statement = ("MATCH (u:`User`{user_id:{A}}) "
             "MATCH (m:`Movie`{movie_id:{C}}) MERGE (u)-[r:`Has_rated`{rating:{B}}]->(m) RETURN r")

# 평점 순회
# Looping over ratings
for r,row in rating.iterrows() :
    # Retrieve "User" and "Movie" nodes, and create relationship with the corresponding rating as property
    tx.append(statement, {"A": row.loc['user_id'], "B": row.loc['rating'], "C": row.loc['item_id']})
    if r%100==0 : tx.process()

tx.commit()

# 인덱스 생성
graph.cypher.execute('CREATE INDEX ON :User(user_id)')
graph.cypher.execute('CREATE INDEX ON :Movie(movie_id)')
graph.cypher.execute('CREATE INDEX ON :Genre(genre_id)')

user_id = 944
threshold = 0.5

# In Strategy 1, the similarity between two users u1 and u2 is the proportion of movies they have in common
# The score of one given movie m is the proportion of users similar to u1 who rated m

query = (### Similarity normalization : count number of movies seen by u1 ###
  # Count movies rated by u1 as countm
  'MATCH (u1:`User` {user_id:{user_id}})-[:`Has_rated`]->(m1:`Movie`) '
  'WITH count(m1) as countm '
  ### Score normalization : count number of users who are considered similar to u1 ###
  # Retrieve all users u2 who share at least one movie with u1
  'MATCH (u1:`User` {user_id:{user_id}})-[:`Has_rated`]->(m1:`Movie`) '
  'MATCH (m1)<-[r:`Has_rated`]-(u2:`User`) '
  'WHERE NOT u2=u1 '
  # Compute similarity
  'WITH u2, countm, tofloat(count(r))/countm as sim '
  # Keep users u2 whose similarity with u1 is above some threshold
  'WHERE sim>{threshold} '
  # Count number of similar users as countu
  'WITH count(u2) as countu, countm '
  ### Recommendation ###
  # Retrieve all users u2 who share at least one movie with u1
  'MATCH (u1:`User` {user_id:{user_id}})-[:`Has_rated`]->(m1:`Movie`) '
  'MATCH (m1)<-[r:`Has_rated`]-(u2:`User`) '
  'WHERE NOT u2=u1 '
  # Compute similarity
  'WITH u1, u2,countu, tofloat(count(r))/countm as sim '
  # Keep users u2 whose similarity with u1 is above some threshold
  'WHERE sim>{threshold} '
  # Retrieve movies m that were rated by at least one similar user, but not by u1
  'MATCH (m:`Movie`)<-[r:`Has_rated`]-(u2) '
  'WHERE NOT (m)<-[:`Has_rated`]-(u1) '
  # Compute score and return the list of suggestions ordered by score
  'RETURN DISTINCT m, tofloat(count(r))/countu as score ORDER BY score DESC ')

tx = graph.cypher.begin()
tx.append(query, {'user_id': user_id, 'threshold': threshold})
result = tx.commit()
print result

# stratedgy 2

user_id = 944
threshold = 0.5

# In Strategy 2, the similarity between two users u1 and u2 is the proportion of movies they have in common
# The score of one movie m is the sum of ratings given by users similar to u1

query = (### Similarity normalization : count number of movies seen by u1 ###
    # Count movies rated by u1 as countm
    'MATCH (m1:`Movie`)<-[:`Has_rated`]-(u1:`User` {user_id:{user_id}}) '
    'WITH count(m1) as countm '
    ### Recommendation ###
    # Retrieve all users u2 who share at least one movie with u1
    'MATCH (u2:`User`)-[r2:`Has_rated`]->(m1:`Movie`)<-[r1:`Has_rated`]-(u1:`User` {user_id:{user_id}}) '
    'WHERE (NOT u2=u1) AND (abs(r2.rating - r1.rating) <= 1) '
    # Compute similarity
    'WITH u1, u2, tofloat(count(DISTINCT m1))/countm as sim '
    # Keep users u2 whose similarity with u1 is above some threshold
    'WHERE sim>{threshold} '
    # Retrieve movies m that were rated by at least one similar user, but not by u1
    'MATCH (m:`Movie`)<-[r:`Has_rated`]-(u2) '
    'WHERE (NOT (m)<-[:`Has_rated`]-(u1)) '
    # Compute score and return the list of suggestions ordered by score
    'RETURN DISTINCT m,tofloat(sum(r.rating)) as score ORDER BY score DESC ')

tx = graph.cypher.begin()
tx.append(query, {'user_id': user_id, 'threshold': threshold})
result = tx.commit()
print result

# stratedgy 3

user_id = 944
threshold = 0.5

# In Strategy 3, the similarity between two users is the proportion of movies for which they gave almost the same rating
# The score of one movie m is the mean rating given by users similar to u1

query = (### Similarity normalization : count number of movies seen by u1 ###
    # Count movies rated by u1 as countm
    'MATCH (m1:`Movie`)<-[:`Has_rated`]-(u1:`User` {user_id:{user_id}}) '
    'WITH count(m1) as countm '
    ### Recommendation ###
    # Retrieve all users u2 who share at least one movie with u1
    'MATCH (u2:`User`)-[r2:`Has_rated`]->(m1:`Movie`)<-[r1:`Has_rated`]-(u1:`User` {user_id:{user_id}}) '
    # Check if the ratings given by u1 and u2 differ by less than 1
    'WHERE (NOT u2=u1) AND (abs(r2.rating - r1.rating) <= 1) '
    # Compute similarity
    'WITH u1, u2, tofloat(count(DISTINCT m1))/countm as sim '
    # Keep users u2 whose similarity with u1 is above some threshold
    'WHERE sim>{threshold} '
    # Retrieve movies m that were rated by at least one similar user, but not by u1
    'MATCH (m:`Movie`)<-[r:`Has_rated`]-(u2) '
    'WHERE (NOT (m)<-[:`Has_rated`]-(u1)) '
    # Compute score and return the list of suggestions ordered by score
    'WITH DISTINCT m, count(r) as n_u, tofloat(sum(r.rating)) as sum_r '
    'WHERE n_u > 1 '
    'RETURN m, sum_r/n_u as score ORDER BY score DESC')

tx = graph.cypher.begin()
tx.append(query, {'user_id': user_id, 'threshold': threshold})
result = tx.commit()
print result