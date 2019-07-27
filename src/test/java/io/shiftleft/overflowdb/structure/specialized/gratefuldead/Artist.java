package io.shiftleft.overflowdb.structure.specialized.gratefuldead;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import io.shiftleft.overflowdb.structure.NodeLayoutInformation;
import io.shiftleft.overflowdb.structure.OverflowDbNode;
import io.shiftleft.overflowdb.structure.OverflowElementFactory;
import io.shiftleft.overflowdb.structure.OverflowNodeProperty;
import io.shiftleft.overflowdb.structure.TinkerGraph;
import io.shiftleft.overflowdb.structure.VertexRef;
import io.shiftleft.overflowdb.structure.VertexRefWithLabel;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Artist extends OverflowDbNode {
  public static final String label = "artist";
  public static final String NAME = "name";

  /* properties */
  private String name;

  protected Artist(VertexRef ref) {
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
      return IteratorUtils.of(new OverflowNodeProperty(this, key, name));
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

  public static OverflowElementFactory.ForNode<Artist> factory = new OverflowElementFactory.ForNode<Artist>() {

    @Override
    public String forLabel() {
      return Artist.label;
    }

    @Override
    public Artist createVertex(VertexRef<Artist> ref) {
      return new Artist(ref);
    }

    @Override
    public Artist createVertex(Long id, TinkerGraph graph) {
      final VertexRef<Artist> ref = createVertexRef(id, graph);
      final Artist node = createVertex(ref);
      ref.setElement(node);
      return node;
    }

    @Override
    public VertexRef<Artist> createVertexRef(Long id, TinkerGraph graph) {
      return new VertexRefWithLabel<>(id, graph, null, Artist.label);
    }
  };
}
