package overflowdb.testdomains.gratefuldead;

import overflowdb.EdgeFactory;
import overflowdb.EdgeLayoutInformation;
import overflowdb.NodeRef;
import overflowdb.Graph;
import overflowdb.Edge;
import overflowdb.PropertyKey;

import java.util.Arrays;
import java.util.HashSet;

public class FollowedBy extends Edge {
  public static final String LABEL = "followedBy";
  public static final String WEIGHT = "weight";
  public static final PropertyKey<Integer> WEIGHT_KEY = new PropertyKey<>(WEIGHT);
  public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList(WEIGHT));

  public Integer weight() {
    return (Integer) property(WEIGHT);
  }

  public FollowedBy(Graph graph, NodeRef outVertex, NodeRef inVertex) {
    super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
  }

  public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

  public static EdgeFactory<FollowedBy> factory = new EdgeFactory<FollowedBy>() {
    @Override
    public String forLabel() {
      return FollowedBy.LABEL;
    }

    @Override
    public FollowedBy createEdge(Graph graph, NodeRef outVertex, NodeRef inVertex) {
      return new FollowedBy(graph, outVertex, inVertex);
    }
  };
}
