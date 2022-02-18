package overflowdb.testdomains.gratefuldead;

import overflowdb.NodeLayoutInformation;
import overflowdb.NodeRef;
import overflowdb.NodeDb;
import scala.Int;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class SongDb extends NodeDb {
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
  public Object property(String key) {
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
  protected void updateSpecificProperty(String key, Object value) {
    if (Song.NAME.equals(key)) {
      this._name = (String) value;
    } else if (Song.SONG_TYPE.equals(key)) {
      this._songType = (String) value;
    } else if (Song.PERFORMANCES.equals(key)) {
      if (value instanceof String) {
        this._performances = Integer.valueOf((String) value);
      } else {
        this._performances = ((Integer) value);
      }
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
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
      Song.label,
      new HashSet<>(Arrays.asList(Song.NAME, Song.SONG_TYPE, Song.PERFORMANCES)),
      Arrays.asList(SungBy.layoutInformation, WrittenBy.layoutInformation, FollowedBy.layoutInformation),
      Arrays.asList(FollowedBy.layoutInformation));
}
