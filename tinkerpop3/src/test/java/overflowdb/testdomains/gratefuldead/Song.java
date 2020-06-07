package overflowdb.testdomains.gratefuldead;

import overflowdb.NodeFactory;
import overflowdb.NodeRef;
import overflowdb.OdbGraph;

public class Song extends NodeRef<SongDb> {
  public static final String label = "song";
  public static final String NAME = "name";
  public static final String SONG_TYPE = "songType";
  public static final String PERFORMANCES = "performances";

  public Song(OdbGraph graph, long id) {
    super(graph, id);
  }

  @Override
  public String label() {
    return Song.label;
  }

  public String name() {
    return get().name();
  }

  public String songType() {
    return get().songType();
  }

  public Integer performances() {
    return get().performances();
  }

  public static NodeFactory<SongDb> factory = new NodeFactory<SongDb>() {
    @Override
    public String forLabel() {
      return Song.label;
    }

    @Override
    public int forLabelId() {
      return SongDb.layoutInformation.labelId;
    }

    @Override
    public SongDb createNode(NodeRef<SongDb> ref) {
      return new SongDb(ref);
    }

    @Override
    public Song createNodeRef(OdbGraph graph, long id) {
      return new Song(graph, id);
    }
  };

}
