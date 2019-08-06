package io.shiftleft.overflowdb.structure.specialized.gratefuldead;

import io.shiftleft.overflowdb.structure.EdgeFactory;
import io.shiftleft.overflowdb.structure.EdgeLayoutInformation;
import io.shiftleft.overflowdb.structure.NodeRef;
import io.shiftleft.overflowdb.structure.OdbGraph;
import io.shiftleft.overflowdb.structure.OdbEdge;

import java.util.Arrays;
import java.util.HashSet;

public class FollowedBy extends OdbEdge {
  public static final String LABEL = "followedBy";
  public static final String WEIGHT = "weight";
  public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList(WEIGHT));

  public Integer weight() {
    return (Integer) property(WEIGHT).value();
  }

  public FollowedBy(OdbGraph graph, NodeRef outVertex, NodeRef inVertex) {
    super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
  }

  public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

  public static EdgeFactory<FollowedBy> factory = new EdgeFactory<FollowedBy>() {
    @Override
    public String forLabel() {
      return FollowedBy.LABEL;
    }

    @Override
    public FollowedBy createEdge(OdbGraph graph, NodeRef outVertex, NodeRef inVertex) {
      return new FollowedBy(graph, outVertex, inVertex);
    }
  };
}
