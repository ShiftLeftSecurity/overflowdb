package io.shiftleft.overflowdb.testdomains.movielens;

import io.shiftleft.overflowdb.NodeFactory;
import io.shiftleft.overflowdb.NodeRef;
import io.shiftleft.overflowdb.OdbGraph;
import io.shiftleft.overflowdb.testdomains.simple.TestNodeDb;

public class Occupation extends NodeRef<OccupationDb> {
  public static final String LABEL = "occupation";

  public static final String NAME = "name";
  public static final String UID = "uid";

  public Occupation(OdbGraph graph, long id) {
    super(graph, id);
  }

  @Override
  public String label() {
    return Occupation.LABEL;
  }

  public String name() {
    return get().name();
  }

  public String uid() {
    return get().uid();
  }

  public static NodeFactory<OccupationDb> factory = new NodeFactory<OccupationDb>() {

    @Override
    public String forLabel() {
      return Occupation.LABEL;
    }

    @Override
    public OccupationDb createNode(NodeRef<OccupationDb> ref) {
      return new OccupationDb(ref);
    }

    @Override
    public Occupation createNodeRef(OdbGraph graph, long id) {
      return new Occupation(graph, id);
    }
  };

}
