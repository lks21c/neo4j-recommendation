# -*- coding: utf-8 -*-

from __future__ import absolute_import
from surprise import Dataset, evaluate, Reader
from surprise import KNNBasic
from collections import namedtuple

#data = Dataset.load_builtin("ml-100k")
BuiltinDataset = namedtuple('BuiltinDataset', ['url', 'path', 'reader_params'])

BUILTIN_DATASETS = {
    'ml-100k':
        BuiltinDataset(
            url='http://files.grouplens.org/datasets/movielens/ml-100k.zip',
            path='./ml-100k/u.data',
            reader_params=dict(line_format='user item rating timestamp',
                               rating_scale=(1, 5),
                               sep='\t')
        )
}
dataset = BUILTIN_DATASETS["ml-100k"]
reader = Reader(**dataset.reader_params)
data = Dataset.load_from_file("./ml-100k/u.data", reader)
trainingSet = data.build_full_trainset()

sim_options = {
    'name': 'cosine',
    'user_based': False
}

knn = KNNBasic(sim_options=sim_options)
knn.train(trainingSet)

testSet = trainingSet.build_anti_testset()
predictions = knn.test(testSet)

from collections import defaultdict

def get_top3_recommendations(predictions, topN=3):
    top_recs = defaultdict(list)
    for uid, iid, true_r, est, _ in predictions:
        top_recs[uid].append((iid, est))
    for uid, user_ratings in top_recs.items():
        user_ratings.sort(key=lambda x: x[1], reverse=True)
        top_recs[uid] = user_ratings[:topN]
    return top_recs

import os, io
def read_item_names():
    """Read the u.item file from MovieLens 100-k dataset and returns a
    mapping to convert raw ids into movie names.
    """
    file_name = (os.path.expanduser('~') + '/.surprise_data/ml-100k/ml-100k/u.item')
    rid_to_name = {}
    with io.open(file_name, 'r', encoding='ISO-8859-1') as f:
        for line in f:
            line = line.split('|')
            rid_to_name[line[0]] = line[1]
    return rid_to_name

top3_recommendations = get_top3_recommendations(predictions)
rid_to_name = read_item_names()
for uid, user_ratings in top3_recommendations.items():
    movie_names = []
    movie_ratings = []
    for iid, user_rating in user_ratings:
        movie_names.append(rid_to_name[iid])
        movie_ratings.append(user_rating)
    print(uid, [name for name in movie_names], [rating for rating in movie_ratings])
