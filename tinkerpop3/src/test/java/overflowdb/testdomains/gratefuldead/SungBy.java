package overflowdb.testdomains.gratefuldead;

import overflowdb.EdgeFactory;
import overflowdb.EdgeLayoutInformation;
import overflowdb.NodeRef;
import overflowdb.OdbGraph;
import overflowdb.OdbEdge;

import java.util.Arrays;
import java.util.HashSet;

public class SungBy extends OdbEdge {
  public static final String LABEL = "sungBy";
  public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList());

  public SungBy(OdbGraph graph, NodeRef outVertex, NodeRef inVertex) {
    super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
  }

  public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

  public static EdgeFactory<SungBy> factory = new EdgeFactory<SungBy>() {
    @Override
    public String forLabel() {
      return SungBy.LABEL;
    }

    @Override
    public SungBy createEdge(OdbGraph graph, NodeRef outVertex, NodeRef inVertex) {
      return new SungBy(graph, outVertex, inVertex);
    }
  };
}
