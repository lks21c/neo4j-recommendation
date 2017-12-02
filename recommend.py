# -*- coding: utf-8 -*-

from py2neo import Graph

graph = Graph('http://neo4j:melon123!@localhost:7474/db/data/')

total = 943
topK = 3
userId = 35

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
tx.append(query, {'user_id1': userId, 'topK': topK})
result = tx.commit()
print result