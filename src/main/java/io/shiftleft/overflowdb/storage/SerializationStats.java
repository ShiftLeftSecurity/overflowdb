/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.shiftleft.overflowdb.storage;

import java.util.Map;

public class SerializationStats {

  private final Map<Integer, Integer> vertexGroupCount;
  private final Map<Integer, Integer> edgeGroupCount;

  public SerializationStats(Map<Integer, Integer> vertexGroupCount, Map<Integer, Integer> edgeGroupCount) {
    this.vertexGroupCount = vertexGroupCount;
    this.edgeGroupCount = edgeGroupCount;
  }

  /** Key: serializationCount; Value: number of elements */
  public Map<Integer, Integer> getVertexGroupCount() {
    return vertexGroupCount;
  }

  /** Key: serializationCount; Value: number of elements */
  public Map<Integer, Integer> getEdgeGroupCount() {
    return edgeGroupCount;
  }

  @Override
  public String toString() {
    return "SerializationStats{" +
        "vertexGroupCount=" + vertexGroupCount +
        ", edgeGroupCount=" + edgeGroupCount +
        '}';
  }
}
