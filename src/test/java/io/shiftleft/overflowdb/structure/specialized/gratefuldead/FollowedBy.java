package io.shiftleft.overflowdb.structure.specialized.gratefuldead;

import io.shiftleft.overflowdb.structure.*;

import java.util.*;

public class FollowedBy extends OverflowDbEdge {
  public static final String LABEL = "followedBy";
  public static final String WEIGHT = "weight";
  public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList(WEIGHT));

  public Integer weight() {
    return (Integer) property(WEIGHT).value();
  }

  public FollowedBy(TinkerGraph graph, VertexRef<OverflowDbNode> outVertex, VertexRef<OverflowDbNode> inVertex) {
    super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
  }

  public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

  public static OverflowElementFactory.ForEdge<FollowedBy> factory = new OverflowElementFactory.ForEdge<FollowedBy>() {
    @Override
    public String forLabel() {
      return FollowedBy.LABEL;
    }

    @Override
    public FollowedBy createEdge(TinkerGraph graph, VertexRef outVertex, VertexRef inVertex) {
      return new FollowedBy(graph, outVertex, inVertex);
    }
  };
}
