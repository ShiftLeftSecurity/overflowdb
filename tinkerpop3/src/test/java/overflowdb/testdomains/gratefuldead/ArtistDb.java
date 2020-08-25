package overflowdb.testdomains.gratefuldead;

import overflowdb.NodeLayoutInformation;
import overflowdb.NodeRef;
import overflowdb.OdbNode;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ArtistDb extends OdbNode {
  protected ArtistDb(NodeRef ref) {
    super(ref);
  }

  private String _name;

  public String name() {
    return _name;
  }

  @Override
  public NodeLayoutInformation layoutInformation() {
    return layoutInformation;
  }

  @Override
  protected Object specificProperty2(String key) {
    if (Artist.NAME.equals(key)) {
      return _name;
    } else {
      return null;
    }
  }

  @Override
  public Map<String, Object> valueMap() {
    Map<String, Object> properties = new HashMap<>();
    if (_name != null) properties.put(Artist.NAME, _name);
    return properties;
  }

  @Override
  protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
    if (Artist.NAME.equals(key)) {
      this._name = (String) value;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
    return property(key);
  }

  @Override
  protected void removeSpecificProperty(String key) {
    if (Artist.NAME.equals(key)) {
      this._name = null;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  public static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      3,
      new HashSet<>(Arrays.asList(Artist.NAME)),
      Arrays.asList(),
      Arrays.asList(SungBy.layoutInformation, WrittenBy.layoutInformation));
}
