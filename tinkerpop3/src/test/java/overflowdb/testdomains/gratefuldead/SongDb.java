package overflowdb.testdomains.gratefuldead;

import overflowdb.NodeLayoutInformation;
import overflowdb.NodeRef;
import overflowdb.OdbNode;
import overflowdb.OdbNodeProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class SongDb extends OdbNode {
  protected SongDb(NodeRef ref) {
    super(ref);
  }

  private String _name;
  private String _songType;
  private Integer _performances;

  public String name() {
    return _name;
  }

  public String songType() {
    return _songType;
  }

  public Integer performances() {
    return _performances;
  }

  @Override
  public NodeLayoutInformation layoutInformation() {
    return layoutInformation;
  }

  @Override
  protected Object specificProperty2(String key) {
    if (Song.NAME.equals(key)) {
      return _name;
    } else if (key == Song.SONG_TYPE) {
      return _songType;
    } else if (key == Song.PERFORMANCES) {
      return _performances;
    } else {
      return null;
    }
  }

  @Override
  public Map<String, Object> valueMap() {
    Map<String, Object> properties = new HashMap<>();
    if (_name != null) properties.put(Song.NAME, _name);
    if (_songType != null) properties.put(Song.SONG_TYPE, _songType);
    if (_performances != null) properties.put(Song.PERFORMANCES, _performances);
    return properties;
  }

  @Override
  protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
    if (Song.NAME.equals(key)) {
      this._name = (String) value;
    } else if (Song.SONG_TYPE.equals(key)) {
      this._songType = (String) value;
    } else if (Song.PERFORMANCES.equals(key)) {
      this._performances = ((Integer) value);
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
    return property(key);
  }


  @Override
  protected void removeSpecificProperty(String key) {
    if (Song.NAME.equals(key)) {
      this._name = null;
    } else if (Song.SONG_TYPE.equals(key)) {
      this._songType = null;
    } else if (Song.PERFORMANCES.equals(key)) {
      this._performances = null;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  public static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      1,
      new HashSet<>(Arrays.asList(Song.NAME, Song.SONG_TYPE, Song.PERFORMANCES)),
      Arrays.asList(SungBy.layoutInformation, WrittenBy.layoutInformation, FollowedBy.layoutInformation),
      Arrays.asList(FollowedBy.layoutInformation));
}
