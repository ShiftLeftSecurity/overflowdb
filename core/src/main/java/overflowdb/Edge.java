package overflowdb;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import overflowdb.util.IteratorUtils;

public abstract class Edge extends Element {
  public abstract NodeRef outNode();

  public abstract NodeRef inNode();

  public abstract Iterator<NodeRef> bothNodes() ;

}
