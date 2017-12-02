# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import matplotlib as plt
from py2neo import Graph

graph = Graph('http://neo4j:melon123!@localhost:7474/db/data/')

total = 943
minSimCnt = 5

for i in range(1, total):
    for j in range(1, total):
        if i != j:
            query = (
                'MATCH (p1:User {user_id:{user_id1}})-[r1:Has_rated]->(m:Movie)<-[r2:Has_rated]-(p2:User {user_id:{user_id2}}) '
                ' with count(m) as cnt '
                ' RETURN * '
            )

            tx = graph.cypher.begin()
            tx.append(query, {'user_id1': i, 'user_id2': j})
            result = tx.commit()
            cnt = result[0][0]['cnt']

            if cnt >= minSimCnt:
                query = (
                    ' MATCH (p1:User {user_id:{user_id1}})-[x:Has_rated]->(m:Movie)<-[y:Has_rated]-(p2:User {user_id:{user_id2}}) '
                    ' WITH SUM(x.rating * y.rating) AS xyDotProduct, SQRT(REDUCE(xDot = 0.0, a IN COLLECT(x.rating) | xDot + a^2)) AS xLength, SQRT(REDUCE(yDot = 0.0, b IN COLLECT(y.rating) | yDot + b^2)) AS yLength, p1, p2 '
                    ' MERGE (p1)-[s:SIMILARITY]-(p2) '
                    ' SET s.similarity = xyDotProduct / (xLength * yLength) '
                )

                # print query

                tx = graph.cypher.begin()
                tx.append(query, {'user_id1': i, 'user_id2': j})
                result = tx.commit()
                print i, " ", j, " similarity inserted."