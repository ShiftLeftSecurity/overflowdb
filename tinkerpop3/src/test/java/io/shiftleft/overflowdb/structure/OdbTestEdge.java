package io.shiftleft.overflowdb.structure;

import java.util.Arrays;
import java.util.HashSet;

public class OdbTestEdge extends OdbEdge {
  public static final String LABEL = "testEdge";
  public static final String LONG_PROPERTY = "longProperty";
  public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList(LONG_PROPERTY));

  public OdbTestEdge(OdbGraph graph, NodeRef outVertex, NodeRef inVertex) {
    super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
  }

  public Long longProperty() {
    return (Long) property(LONG_PROPERTY).value();
  }

  public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

  public static OdbElementFactory.ForEdge<OdbTestEdge> factory = new OdbElementFactory.ForEdge<OdbTestEdge>() {
    @Override
    public String forLabel() {
      return OdbTestEdge.LABEL;
    }

    @Override
    public OdbTestEdge createEdge(OdbGraph graph, NodeRef outVertex, NodeRef inVertex) {
      return new OdbTestEdge(graph, outVertex, inVertex);
    }
  };
}
