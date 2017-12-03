# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import matplotlib as plt
from py2neo import Graph

graph = Graph('http://neo4j:melon123!@localhost:7474/db/data/')

test_data = 'ml-100k/u1.test'

def graph_knn(userId):
    topK = 3
    query = (
            'MATCH (b:User)-[r:Has_rated]->(m:Movie), (b)-[s:SIMILARITY]-(a:User {user_id:{user_id1}}) '
            ' WHERE NOT((a)-[:Has_rated]->(m)) '
            ' WITH m, s.similarity AS similarity, r.rating AS rating '
            ' ORDER BY m.title, similarity DESC '
            ' WITH m.title AS movie, COLLECT(rating)[0..{topK}] AS ratings '
            ' WITH movie, REDUCE(s = 0, i IN ratings | s + i)*1.0 / LENGTH(ratings) AS reco '
            ' ORDER BY reco DESC '
            ' RETURN movie AS Movie, reco AS Recommendation '
            )

    tx = graph.cypher.begin()
    tx.append(query, {'user_id1': np.asscalar(user_id), 'topK': topK})
    result = tx.commit()
    return result

def predit_rating(user_id):
    threshold = 0.5
    # stratedgy 3
    # In Strategy 3, the similarity between two users is the proportion of movies for which they gave almost the same rating
    # The score of one movie m is the mean rating given by users similar to u1
    print ("Strategy #3 %s" % str(user_id))
    query = (  ### Similarity normalization : count number of movies seen by u1 ###
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
    tx.append(query, {'user_id': np.asscalar(user_id), 'threshold': threshold})
    result = tx.commit()
    #print result
    return result

rating_col = ['user_id', 'item_id','rating', 'timestamp']
rating = pd.read_csv(test_data, sep='\t' ,header=None, names=rating_col)
n_r = rating.shape[0]

cur_user_id = None
cur_prediction = None

#user_id, item_id, real_rating, find_rating
result_list = []

movie_col = ['id', 'title','release date', 'useless', 'IMDb url']
movie = pd.read_csv('ml-100k/u.item', sep='|', header=None, names=movie_col)
movie = movie.fillna('unknown')

for r, row in rating.iterrows():
    user_id = row.loc['user_id']
    item_id = row.loc['item_id']
    real_rating = row.loc['rating']
    print user_id, item_id, real_rating
    if cur_user_id is None:
        cur_user_id = row.loc['user_id']
        #cur_prediction = predit_rating(cur_user_id)
        cur_prediction = graph_knn(cur_user_id)
    elif cur_user_id != user_id:
        cur_user_id = row.loc['user_id']
        #cur_prediction = predit_rating(cur_user_id)
        cur_prediction = graph_knn(cur_user_id)

    """
    # for predit_rating
    if cur_prediction is not None:
        result = cur_prediction[0]  # -> this is a Record object
        list_of_result = list(result)
        find_rating = -1
        for result in list_of_result:
            if result['m']['movie_id'] == item_id:
                find_rating = result['score']

        result_list.append((user_id, item_id, real_rating, find_rating))
    """
    # for grahp_knn
    if cur_prediction is not None:
        result = cur_prediction[0]  # -> this is a Record object
        list_of_result = list(result)
        find_rating = -1
        cur_title = None
        for m, row in movie.iterrows():
            # Create "Movie" node
            if m[0] == item_id:
                cur_title = m[1]
                break
        if cur_title is not None:
            for result in list_of_result:
                if result['Movie'] == cur_title:
                    find_rating = result['Recommendation']
                    break

        result_list.append((user_id, item_id, real_rating, find_rating))

target_list = np.array([])
predict_list = np.array([])
for user_id, item_id, real_rating, find_rating in result_list:
    if find_rating != -1:
        target_list = np.append(target_list, real_rating)
        predict_list = np.append(predict_list, find_rating)

print np.sqrt(np.mean((target_list-predict_list)**2))
