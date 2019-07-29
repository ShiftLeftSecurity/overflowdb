package io.shiftleft.overflowdb.storage;

import java.util.Map;

public class SerializationStats {

  private final Map<Integer, Integer> vertexGroupCount;
  private final Map<Integer, Integer> edgeGroupCount;

  public SerializationStats(Map<Integer, Integer> vertexGroupCount, Map<Integer, Integer> edgeGroupCount) {
    this.vertexGroupCount = vertexGroupCount;
    this.edgeGroupCount = edgeGroupCount;
  }

  /**
   * Key: serializationCount; Value: number of elements
   */
  public Map<Integer, Integer> getVertexGroupCount() {
    return vertexGroupCount;
  }

  /**
   * Key: serializationCount; Value: number of elements
   */
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
