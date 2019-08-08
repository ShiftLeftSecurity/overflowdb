package io.shiftleft.overflowdb.testdomains.gratefuldead;

import io.shiftleft.overflowdb.NodeFactory;
import io.shiftleft.overflowdb.NodeLayoutInformation;
import io.shiftleft.overflowdb.NodeRef;
import io.shiftleft.overflowdb.OdbGraph;
import io.shiftleft.overflowdb.OdbNode;
import io.shiftleft.overflowdb.OdbNodeProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class Artist extends OdbNode {
  public static final String label = "artist";
  public static final String NAME = "name";

  /* properties */
  private String name;

  protected Artist(NodeRef ref) {
    super(ref);
  }

  @Override
  public String label() {
    return Artist.label;
  }

  @Override
  protected NodeLayoutInformation layoutInformation() {
    return layoutInformation;
  }

  public String getName() {
    return name;
  }

  @Override
  protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
    final VertexProperty<V> ret;
    if (NAME.equals(key) && name != null) {
      return IteratorUtils.of(new OdbNodeProperty(this, key, name));
    } else {
      return Collections.emptyIterator();
    }
  }

  @Override
  public Map<String, Object> valueMap() {
    Map<String, Object> properties = new HashMap<>();
    if (name != null) properties.put(NAME, name);
    return properties;
  }

  @Override
  protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
    if (NAME.equals(key)) {
      this.name = (String) value;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
    return property(key);
  }

  @Override
  protected void removeSpecificProperty(String key) {
    if (NAME.equals(key)) {
      this.name = null;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  private static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      new HashSet<>(Arrays.asList(NAME)),
      Arrays.asList(),
      Arrays.asList(SungBy.layoutInformation, WrittenBy.layoutInformation));

  public static NodeFactory<Artist> factory = new NodeFactory<Artist>() {

    @Override
    public String forLabel() {
      return Artist.label;
    }

    @Override
    public Artist createNode(NodeRef<Artist> ref) {
      return new Artist(ref);
    }

    @Override
    public NodeRef<Artist> createNodeRef(OdbGraph graph, long id) {
      return new NodeRef(graph, id) {
        @Override
        public String label() {
          return Artist.label;
        }
      };
    }
  };
}
