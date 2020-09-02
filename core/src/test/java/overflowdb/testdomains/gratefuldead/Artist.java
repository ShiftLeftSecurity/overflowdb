package overflowdb.testdomains.gratefuldead;

import overflowdb.NodeFactory;
import overflowdb.NodeRef;
import overflowdb.Graph;

public class Artist extends NodeRef<ArtistDb> {
  public static final String label = "artist";
  public static final String NAME = "name";

  public Artist(Graph graph, long id) {
    super(graph, id);
  }

  public String name() {
    return get().name();
  }

  @Override
  public String label() {
    return Artist.label;
  }

  public static NodeFactory<ArtistDb> factory = new NodeFactory<ArtistDb>() {
    @Override
    public String forLabel() {
      return Artist.label;
    }

    @Override
    public int forLabelId() {
      return ArtistDb.layoutInformation.labelId;
    }

    @Override
    public ArtistDb createNode(NodeRef<ArtistDb> ref) {
      return new ArtistDb(ref);
    }

    @Override
    public Artist createNodeRef(Graph graph, long id) {
      return new Artist(graph, id);
    }
  };
}
