<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

[![Build Status](https://travis-ci.org/ShiftLeftSecurity/tinkergraph-gremlin.svg?branch=master)](https://travis-ci.org/ShiftLeftSecurity/tinkergraph-gremlin)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/tinkergraph-gremlin/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/tinkergraph-gremlin)

# ShiftLeft TinkerGraph
This is Fork of [Apache TinkerGraph](https://github.com/apache/tinkerpop/tree/master/tinkergraph-gremlin) that uses uses 70% less memory (for our use case, ymmv) and implements a strict schema validation. Related blog article on [ShiftLeft Blog](https://blog.shiftleft.io/open-sourcing-our-specialized-tinkergraph-with-70-memory-reduction-and-strict-schema-validation-fa5cfb3dd82d)

## Usage
1) add a dependency to the latest published artifact on [maven central](https://maven-badges.herokuapp.com/maven-central/io.shiftleft/tinkergraph-gremlin)
2) extend [SpecializedTinkerVertex](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedTinkerVertex.java) for vertices and [SpecializedTinkerEdge](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedTinkerEdge.java) for edges
3) create instances of [`SpecializedElementFactory.ForVertex`](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedElementFactory.java#L29) and [`SpecializedElementFactory.ForEdge`](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedElementFactory.java#L34) and pass them to [`TinkerGraph.open`](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/TinkerGraph.java#L153-L156)

The repository contains examples for the [grateful dead graph](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/tree/master/src/test/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/specialized/gratefuldead) and there is a [full test setup](https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/blob/master/src/test/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/SpecializedElementsTest.java#L41-L51) that uses them.
2) and 3) are basically boilerplate and therefor good candidates for code generation. 

Other than that, it's a minimally invasive operation, because all other graph and traversal APIs remain the same, i.e., you won't need to change any of your queries. We didn't encounter a single issue when we deployed this into production. 

# Motivation and context
The main difference is that instead of generic HashMaps we use specific structures as per your domain. To make this more clear, let's look at the main use cases for HashMaps in TinkerGraph:

1) allow any vertex and any edge to have any property (basically a key/value pair, e.g., `foo=42`). To achieve this, each element in the graph has a `Map<String, Property>`, and each property is wrapped inside a `HashMap$Node`, see [TinkerVertex](https://github.com/apache/tinkerpop/blob/3.3.0/tinkergraph-gremlin/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/TinkerVertex.java#L45) and [TinkerEdge](https://github.com/apache/tinkerpop/blob/3.3.0/tinkergraph-gremlin/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/TinkerEdge.java#L43). 
2) TinkerGraph allows to connect any two vertices by any edge. Therefor each vertex holds two `Map<String, Set<Edge>>` instances (one for incoming and one for outgoing edges), where the String refers to the edge label.

Being generic and not enforcing a schema makes complete sense for the default TinkerGraph - it allows users to play without restrictions and build prototypes. Once a project is more mature though, chances are you have a good understanding of your domain and can define a schema, so that you don't need the generic structure any more and can save a lot of memory.

Using less memory is not the only benefit, though: knowing exactly which properties a given element can have, of which type they are and which edges are allowed on a specific vertex, helps catching errors very early in the development cycle. Your IDE can help you to build valid (i.e., schema conforming) graphs and traversals. If you use a statically-checked language, your compiler can find errors that would otherwise only occur at runtime. Even if you are using a dynamic language you are better off, because you'll get an error when you load the graph, e.g., by setting a property on the wrong vertex type. This is far better than getting invalid results at query time, when you need to debug all the way back to a potentially very simple mistake. Since we already had a loosely-defined schema for our code property graph, this exercise helped to complete and strengthen it.

## What does this mean in practice?
'Enforcing a strict schema' actually translates to something very simple: we just replaced the *generic* HashMaps with *specific* members:

1) Element properties: vertices and edges contain *generic* `HashMap<String, Object>` that hold all the element's properties. We just replaced them with *specific* class members, e.g., `String name` and `String return_type`

2) Edges on a vertex: the *generic* TinkerVertex contains two `HashMap<String, Set<Edge>> in|outEdges` which can reference any edge. We replaced these by *specific* `Set<SomeSpecificEdgeType>` for each edge type that is allowed to connect this vertex with another vertex.

This means that we can throw an error if the schema is violated, e.g., if a the user tries to set a property that is not defined for a specific vertex, or if the user tris to connect a vertex via an edge that's not supposed to be connected to this vertex. 
It is important to note though, that it's up to you if you want to make this a strict validation or not - you can choose to tolerate schema violations in your domain classes.

## Limitations
* indices aren't updated automatically when you mutate or add elements to the graph. This would be easy to do I guess, but we haven't had the need yet. Workaround: drop and recreate the index.
* an OLAP (GraphComputer) implementation is available, but we haven't really tested it yet
* you cannot (yet) mix generic and specialized Elements: it's all or nothing, and you'll get an error if you accidentally try

# Bring in changes from upstream TinkerGraph
When a new Apache TinkerGraph is being released, here's the steps to bring them into this fork:

```
# view diff
cd ~/Projects/tinkerpop/tinkerpop3
git diff 3.3.2..3.3.3 tinkergraph-gremlin/src > ~/tp-upgrade.patch
# apply patch (-p2 strips the base directory, which is different in our fork)
cd ~/Projects/shiftleft/tinkergraph-gremlin
git apply -p2 ~/tp-upgrade.patch
# manually fix all conflicts (*.orig / *.rej files)
# update all versions in pom.xml
mvn clean test
```

# Release instructions
* change the version in `pom.xml` to a non-snapshot (e.g. `3.3.0.3`)
* commit and tag it (e.g. `v3.3.0.3`), push everything (including the tag!)
* await [Travis](https://travis-ci.org/ShiftLeftSecurity/tinkergraph-gremlin) to automatically deploy the tagged version to [sonatype](https://oss.sonatype.org/content/repositories/public/io/shiftleft/tinkergraph-gremlin/) and stage it so that it'll be synchronized to [maven central](https://repo1.maven.org/maven2/io/shiftleft/tinkergraph-gremlin/) within a few hours. Note: check the log output of the last travis step (`$ ./travis/deploy.sh`) to be sure. You should see something like the following at the very end:
```
[INFO] Remote staged 1 repositories, finished with success.
[INFO] Remote staging repositories are being released...
Waiting for operation to complete...
............
[INFO] Remote staging repositories released.
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------

```
* change the version to the next snapshot (e.g. `3.3.0.4-SNAPSHOT`)
