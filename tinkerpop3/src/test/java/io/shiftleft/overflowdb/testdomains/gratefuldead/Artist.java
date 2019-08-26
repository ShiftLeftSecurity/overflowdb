package io.shiftleft.overflowdb.testdomains.gratefuldead;

import io.shiftleft.overflowdb.NodeFactory;
import io.shiftleft.overflowdb.NodeRef;
import io.shiftleft.overflowdb.OdbGraph;

public class Artist extends NodeRef<ArtistDb> {
  public static final String label = "artist";
  public static final String NAME = "name";

  public Artist(OdbGraph graph, long id) {
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
    public ArtistDb createNode(NodeRef<ArtistDb> ref) {
      return new ArtistDb(ref);
    }

    @Override
    public Artist createNodeRef(OdbGraph graph, long id) {
      return new Artist(graph, id);
    }
  };
}
