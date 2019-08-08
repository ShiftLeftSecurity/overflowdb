package io.shiftleft.overflowdb.structure;

import io.shiftleft.overflowdb.testdomains.gratefuldead.Artist;
import io.shiftleft.overflowdb.testdomains.gratefuldead.FollowedBy;
import io.shiftleft.overflowdb.testdomains.gratefuldead.GratefulDead;
import io.shiftleft.overflowdb.testdomains.gratefuldead.Song;
import io.shiftleft.overflowdb.testdomains.gratefuldead.SungBy;
import io.shiftleft.overflowdb.testdomains.gratefuldead.WrittenBy;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TraversalOptimizationTest {

  @Test
  public void optimizationStrategyAffectedSteps() throws IOException {
    try (OdbGraph graph = GratefulDead.newGraphWithData()) {
      // using `g.V().hasLabel(lbl)` optimization
      assertEquals(584, graph.traversal().V().hasLabel(Song.label).toList().size());
      assertEquals(142, graph.traversal().V().has(Song.PERFORMANCES, 1).toList().size());
      assertEquals(142, graph.traversal().V().has(Song.PERFORMANCES, 1).hasLabel(Song.label).toList().size());
      assertEquals(142, graph.traversal().V().hasLabel(Song.label).has(Song.PERFORMANCES, 1).toList().size());
      assertEquals(7047, graph.traversal().V().out().hasLabel(Song.label).toList().size());
      assertEquals(1, graph.traversal().V(800l).hasLabel(Song.label).toList().size());
      assertEquals(5, graph.traversal().V(1l).out().hasLabel(Song.label).toList().size());
      assertEquals(0, graph.traversal().V().hasLabel(Song.label).hasLabel(Artist.label).toList().size());
      assertEquals(808, graph.traversal().V().hasLabel(Song.label, Artist.label).toList().size());
      assertEquals(501, graph.traversal().V().outE().hasLabel(WrittenBy.LABEL).toList().size());
      assertEquals(501, graph.traversal().V().hasLabel(Song.label).outE().hasLabel(WrittenBy.LABEL).toList().size());

      // using `g.E().hasLabel(lbl)` optimization
      assertEquals(8049, graph.traversal().E().toList().size());
      assertEquals(7047, graph.traversal().E().hasLabel(FollowedBy.LABEL).toList().size());
      assertEquals(3564, graph.traversal().E().has(FollowedBy.WEIGHT, 1).toList().size());
      assertEquals(3564, graph.traversal().E().hasLabel(FollowedBy.LABEL).has(FollowedBy.WEIGHT, 1).toList().size());
      assertEquals(3564, graph.traversal().E().has(FollowedBy.WEIGHT, 1).hasLabel(FollowedBy.LABEL).toList().size());
      assertEquals(7047, graph.traversal().E().hasLabel(FollowedBy.LABEL).outV().hasLabel(Song.label).toList().size());
      assertEquals(7548, graph.traversal().E().hasLabel(FollowedBy.LABEL, SungBy.LABEL).toList().size());
    }
  }

}
