package io.shiftleft.overflowdb;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;

public class OdbGraphBuilder {
  private final OdbGraph graph;
  TLongObjectMap<NodeRef> newNodes = new TLongObjectHashMap<>();
  TLongObjectHashMap<ArrayList<EdgeInfo>> edges = new TLongObjectHashMap<>();
  TLongObjectMap<ArrayList<PropertyInfo>> nodeProperties = new TLongObjectHashMap<>();

  static class EdgeInfo {
    final long outNodeId, inNodeId;
    final String label;
    final Object[] keyValues;

    public EdgeInfo(long outNodeId, long inNodeId, String label, Object[] keyValues) {
      this.outNodeId = outNodeId;
      this.inNodeId = inNodeId;
      this.label = label;
      this.keyValues = keyValues;
    }
  }

  static class PropertyInfo {
    final long nodeId;
    final VertexProperty.Cardinality cardinality;
    final String key;
    final Object value;

    public PropertyInfo(long nodeId, VertexProperty.Cardinality cardinality, String key, Object value) {
      this.nodeId = nodeId;
      this.cardinality = cardinality;
      this.key = key;
      this.value = value;
    }
  }

  public OdbGraphBuilder(OdbGraph graph) {
    this.graph = graph;
  }

  public long addVertex(final Object... keyValues) {
    final NodeRef node = graph.createVertex(keyValues);
    this.newNodes.put(node.id, node);
    return node.id;
  }

  public void addEdge(long outNodeId, long inNodeId, String label, Object... keyValues) {
    final EdgeInfo edge = new EdgeInfo(outNodeId, inNodeId, label, keyValues);
    addEdgeToMap(edges, outNodeId, edge);
    addEdgeToMap(edges, inNodeId, edge);
  }

  public void addVertexProperty(long nodeId, VertexProperty.Cardinality cardinality, String key, Object value) {
    final NodeRef nodeRef = newNodes.get(nodeId);
    if (nodeRef != null) {
      nodeRef.property(cardinality, key, value);
    } else {
      PropertyInfo propertyInfo = new PropertyInfo(nodeId, cardinality, key, value);
      ArrayList<PropertyInfo> propertyInfoList = this.nodeProperties.get(nodeId);
      if (propertyInfoList == null) {
        propertyInfoList = new ArrayList<>();
        this.nodeProperties.put(nodeId, propertyInfoList);
      }
      propertyInfoList.add(propertyInfo);
    }
  }

  private void addEdgeToMap(TLongObjectHashMap<ArrayList<EdgeInfo>> map, long nodeId, EdgeInfo edgeInfo) {
    ArrayList<EdgeInfo> edgeInfos = map.get(nodeId);
    if (edgeInfos == null) {
      edgeInfos = new ArrayList<>();
      map.put(nodeId, edgeInfos);
    }
    edgeInfos.add(edgeInfo);
  }

  public void appendToGraph() {
    graph.appendFromBuilder(this);
  }


}
