package io.shiftleft.overflowdb.testdomains.movielens;

import io.shiftleft.overflowdb.EdgeFactory;
import io.shiftleft.overflowdb.EdgeLayoutInformation;
import io.shiftleft.overflowdb.NodeRef;
import io.shiftleft.overflowdb.OdbEdge;
import io.shiftleft.overflowdb.OdbGraph;

import java.util.Arrays;
import java.util.HashSet;

public class HasOccupation extends OdbEdge {
  public static final String LABEL = "occupation";
  public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList());

  public HasOccupation(OdbGraph graph, NodeRef outVertex, NodeRef inVertex) {
    super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
  }

  public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

  public static EdgeFactory<HasOccupation> factory = new EdgeFactory<HasOccupation>() {
    @Override
    public String forLabel() {
      return HasOccupation.LABEL;
    }

    @Override
    public HasOccupation createEdge(OdbGraph graph, NodeRef outVertex, NodeRef inVertex) {
      return new HasOccupation(graph, outVertex, inVertex);
    }
  };
}
