package io.shiftleft.overflowdb;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;

public class OdbGraphBuilder {
  private final OdbGraph graph;
  protected boolean applied = false;
  protected TLongObjectMap<NodeRef> nodes = new TLongObjectHashMap<>();
  protected TLongObjectHashMap<ArrayList<EdgeInfo>> edges = new TLongObjectHashMap<>();
  protected TLongObjectMap<ArrayList<NodePropertyInfo>> nodeProperties = new TLongObjectHashMap<>();
  protected ArrayList<EdgePropertyInfo> edgeProperties = new ArrayList<>();

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

  static class NodePropertyInfo {
    final long nodeId;
    final VertexProperty.Cardinality cardinality;
    final String key;
    final Object value;

    public NodePropertyInfo(long nodeId, VertexProperty.Cardinality cardinality, String key, Object value) {
      this.nodeId = nodeId;
      this.cardinality = cardinality;
      this.key = key;
      this.value = value;
    }
  }

  static class EdgePropertyInfo {
    final long outNodeId, inNodeId;
    final String label;
    final String key;
    final Object value;

    public EdgePropertyInfo(long outNodeId, long inNodeId, String label, String key, Object value) {
      this.outNodeId = outNodeId;
      this.inNodeId = inNodeId;
      this.label = label;
      this.key = key;
      this.value = value;
    }
  }

  public OdbGraphBuilder(OdbGraph graph) {
    this.graph = graph;
  }

  public long addVertex(final Object... keyValues) {
    final NodeRef node = graph.createVertex(keyValues);
    this.nodes.put(node.id, node);
    return node.id;
  }

  public void addEdge(long outNodeId, long inNodeId, String label, Object... keyValues) {
    final EdgeInfo edge = new EdgeInfo(outNodeId, inNodeId, label, keyValues);
    addEdgeToMap(edges, outNodeId, edge);
    addEdgeToMap(edges, inNodeId, edge);
  }

  public void addVertexProperty(long nodeId, VertexProperty.Cardinality cardinality, String key, Object value) {
    final NodeRef nodeRef = nodes.get(nodeId);
    if (nodeRef != null) {
      nodeRef.property(cardinality, key, value);
    } else {
      NodePropertyInfo nodePropertyInfo = new NodePropertyInfo(nodeId, cardinality, key, value);
      ArrayList<NodePropertyInfo> nodePropertyInfoList = this.nodeProperties.get(nodeId);
      if (nodePropertyInfoList == null) {
        nodePropertyInfoList = new ArrayList<>();
        this.nodeProperties.put(nodeId, nodePropertyInfoList);
      }
      nodePropertyInfoList.add(nodePropertyInfo);
    }
  }

  public void addEdgeProperty(long outNodeId, long inNodeId, String label, String key, Object value) {
    edgeProperties.add(new EdgePropertyInfo(outNodeId, inNodeId, label, key, value));
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
