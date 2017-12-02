from __future__ import absolute_import
import surprise

data = surprise.Dataset.load_builtin('ml-100k')
data.split(n_folds=5)

algo = surprise.BaselineOnly()

for trainset, testset in data.folds():

    # train and test algorithm.
    algo.train(trainset)
    predictions = algo.test(testset)

    # Compute and print Root Mean Squared Error
    rmse = surprise.accuracy.rmse(predictions, verbose=True)
    print(rmse)


"""
data = Dataset.load_builtin('ml-100k')

# Retrieve the trainset.
trainset = data.build_full_trainset()

# Build an algorithm, and train it.
algo = KNNBasic()
algo.train(trainset)

uid = str(196)  # raw user id (as in the ratings file). They are **strings**!
iid = str(302)  # raw item id (as in the ratings file). They are **strings**!

# get a prediction for specific users and items.
pred = algo.predict(uid, iid, r_ui=4, verbose=True)
"""
