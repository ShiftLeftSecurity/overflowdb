### Run all (e.g. on graphripper)
```
overflowdb/target/universal/stage/bin/overflowdb | tee overflowdb.log
tinkergraph/target/universal/stage/bin/tinkergraph | tee tinkergraph.log
neo4j/target/universal/stage/bin/neo4j | tee neo4j.log
janusgraph/target/universal/stage/bin/janusgraph | tee janusgraph.log
orientdb/target/universal/stage/bin/orientdb | tee orientdb.log
```

### Results (31/08/2018): average ms
```
   OverflowDB|TinkerGraph|JanusGraph|  Neo4j  |OrientDb
1)      46          47         83         87        94
2)      46          46         80         83        83
3)    1485        1861      13644       3468     20178
4)     6.5           7         22          8        29
5)     198         195        213         91        89
6)      60          64        584        188       563
7)     169         180        686        310       711
```

Traversals:
1) g.V.outE.inV.outE.inV.outE.inV
2) g.V.out.out.out
3) g.V.out.out.out.path
4) g.V.repeat(out()).times(2)
5) g.V.repeat(out()).times(3)
6) g.V.local(out().out().values("name").fold)
7) g.V.out.local(out.out.values("name").fold)
