# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import matplotlib as plt
from py2neo import Graph

graph = Graph('http://neo4j:neo4j@localhost:7474/db/data/')

test_data = 'ml-100k/u1.test'

def predit_rating(user_id):
    threshold = 0.5
    # stratedgy 3
    # In Strategy 3, the similarity between two users is the proportion of movies for which they gave almost the same rating
    # The score of one movie m is the mean rating given by users similar to u1
    print "Strategy #3"
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
        'RETURN m, sum_r/n_u as score ORDER BY score DESC limit 10')

    tx = graph.cypher.begin()
    tx.append(query, {'user_id': np.asscalar(user_id), 'threshold': threshold})
    result = tx.commit()
    print result
    return result

rating_col = ['user_id', 'item_id','rating', 'timestamp']
rating = pd.read_csv(test_data, sep='\t' ,header=None, names=rating_col)
n_r = rating.shape[0]

for r,row in rating.iterrows() :
    user_id = row.loc['user_id']
    real_rating = row.loc['rating']
    item_id = row.loc['item_id']
    prediction = predit_rating(user_id)
    if len(prediction) > 1:
        print prediction
