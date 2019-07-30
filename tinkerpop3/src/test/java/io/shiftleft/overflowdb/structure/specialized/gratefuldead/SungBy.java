package io.shiftleft.overflowdb.structure.specialized.gratefuldead;

import io.shiftleft.overflowdb.structure.EdgeLayoutInformation;
import io.shiftleft.overflowdb.structure.NodeRef;
import io.shiftleft.overflowdb.structure.OverflowDbGraph;
import io.shiftleft.overflowdb.structure.OverflowDbEdge;
import io.shiftleft.overflowdb.structure.OverflowElementFactory;

import java.util.Arrays;
import java.util.HashSet;

public class SungBy extends OverflowDbEdge {
  public static final String LABEL = "sungBy";
  public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList());

  public SungBy(OverflowDbGraph graph, NodeRef outVertex, NodeRef inVertex) {
    super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
  }

  public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

  public static OverflowElementFactory.ForEdge<SungBy> factory = new OverflowElementFactory.ForEdge<SungBy>() {
    @Override
    public String forLabel() {
      return SungBy.LABEL;
    }

    @Override
    public SungBy createEdge(OverflowDbGraph graph, NodeRef outVertex, NodeRef inVertex) {
      return new SungBy(graph, outVertex, inVertex);
    }
  };
}
