package io.shiftleft.overflowdb.structure;

import java.util.Arrays;
import java.util.HashSet;

public class OverflowDbTestEdge extends OverflowDbEdge {
  public static final String LABEL = "testEdge";
  public static final String LONG_PROPERTY = "longProperty";
  public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList(LONG_PROPERTY));

  public OverflowDbTestEdge(OverflowDb graph, NodeRef outVertex, NodeRef inVertex) {
    super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
  }

  public Long longProperty() {
    return (Long) property(LONG_PROPERTY).value();
  }

  public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

  public static OverflowElementFactory.ForEdge<OverflowDbTestEdge> factory = new OverflowElementFactory.ForEdge<OverflowDbTestEdge>() {
    @Override
    public String forLabel() {
      return OverflowDbTestEdge.LABEL;
    }

    @Override
    public OverflowDbTestEdge createEdge(OverflowDb graph, NodeRef outVertex, NodeRef inVertex) {
      return new OverflowDbTestEdge(graph, outVertex, inVertex);
    }
  };
}
