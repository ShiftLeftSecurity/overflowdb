package io.shiftleft.overflowdb.testdomains.gratefuldead;

import io.shiftleft.overflowdb.structure.NodeFactory;
import io.shiftleft.overflowdb.structure.NodeLayoutInformation;
import io.shiftleft.overflowdb.structure.NodeRef;
import io.shiftleft.overflowdb.structure.OdbGraph;
import io.shiftleft.overflowdb.structure.OdbNode;
import io.shiftleft.overflowdb.structure.OdbNodeProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class Song extends OdbNode {
  public static final String label = "song";
  public static final String NAME = "name";
  public static final String SONG_TYPE = "songType";
  public static final String PERFORMANCES = "performances";
  public static final String TEST_PROP = "testProperty";

  /* properties */
  private String name;
  private String songType;
  private Integer performances;
  private int[] testProp;

  protected Song(NodeRef ref) {
    super(ref);
  }

  @Override
  public String label() {
    return Song.label;
  }

  @Override
  protected NodeLayoutInformation layoutInformation() {
    return layoutInformation;
  }

  public String getName() {
    return name;
  }

  public String getSongType() {
    return songType;
  }

  public Integer getPerformances() {
    return performances;
  }

  @Override
  protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
    final VertexProperty<V> ret;
    if (NAME.equals(key) && name != null) {
      return IteratorUtils.of(new OdbNodeProperty(this, key, name));
    } else if (key == SONG_TYPE && songType != null) {
      return IteratorUtils.of(new OdbNodeProperty(this, key, songType));
    } else if (key == PERFORMANCES && performances != null) {
      return IteratorUtils.of(new OdbNodeProperty(this, key, performances));
    } else if (key == TEST_PROP && testProp != null) {
      return IteratorUtils.of(new OdbNodeProperty(this, key, testProp));
    } else {
      return Collections.emptyIterator();
    }
  }

  @Override
  public Map<String, Object> valueMap() {
    Map<String, Object> properties = new HashMap<>();
    if (name != null) properties.put(NAME, name);
    if (songType != null) properties.put(SONG_TYPE, songType);
    if (performances != null) properties.put(PERFORMANCES, performances);
    if (testProp != null) properties.put(TEST_PROP, testProp);
    return properties;
  }

  @Override
  protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
    if (NAME.equals(key)) {
      this.name = (String) value;
    } else if (SONG_TYPE.equals(key)) {
      this.songType = (String) value;
    } else if (PERFORMANCES.equals(key)) {
      this.performances = ((Integer) value);
    } else if (TEST_PROP.equals(key)) {
      this.testProp = (int[]) value;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
    return property(key);
  }


  @Override
  protected void removeSpecificProperty(String key) {
    if (NAME.equals(key)) {
      this.name = null;
    } else if (SONG_TYPE.equals(key)) {
      this.songType = null;
    } else if (PERFORMANCES.equals(key)) {
      this.performances = null;
    } else if (TEST_PROP.equals(key)) {
      this.testProp = null;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  private static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      new HashSet<>(Arrays.asList(NAME, SONG_TYPE, PERFORMANCES, TEST_PROP)),
      Arrays.asList(SungBy.layoutInformation, WrittenBy.layoutInformation, FollowedBy.layoutInformation),
      Arrays.asList(FollowedBy.layoutInformation));

  public static NodeFactory<Song> factory = new NodeFactory<Song>() {
    @Override
    public String forLabel() {
      return Song.label;
    }

    @Override
    public Song createNode(NodeRef<Song> ref) {
      return new Song(ref);
    }

    @Override
    public NodeRef<Song> createNodeRef(OdbGraph graph, long id) {
      return new NodeRef(graph, id) {
        @Override
        public String label() {
          return Song.label;
        }
      };
    }
  };

}
