package overflowdb.testdomains.gratefuldead;

import overflowdb.EdgeFactory;
import overflowdb.EdgeLayoutInformation;
import overflowdb.NodeRef;
import overflowdb.OdbGraph;
import overflowdb.Edge;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;

public class WrittenBy extends Edge implements Serializable {
  public static final String LABEL = "writtenBy";
  public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList());

  public WrittenBy(OdbGraph graph, NodeRef outVertex, NodeRef inVertex) {
    super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
  }

  public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

  public static EdgeFactory<WrittenBy> factory = new EdgeFactory<WrittenBy>() {
    @Override
    public String forLabel() {
      return WrittenBy.LABEL;
    }

    @Override
    public WrittenBy createEdge(OdbGraph graph, NodeRef outVertex, NodeRef inVertex) {
      return new WrittenBy(graph, outVertex, inVertex);
    }
  };
}
