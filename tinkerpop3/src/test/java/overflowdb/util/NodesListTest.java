package overflowdb.util;

import org.junit.Test;
import overflowdb.NodeRef;
import overflowdb.OdbConfig;
import overflowdb.OdbGraph;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodesListTest {

  @Test
  public void addElements() {
    NodesList nl = new NodesList();

    NodeRef ref1 = dummyRef(1L, "A");
    NodeRef ref2 = dummyRef(2L, "A");
    NodeRef ref3 = dummyRef(3L, "B");
    nl.add(ref1);
    nl.add(ref2);
    nl.add(ref3);

    assertEquals(3, nl.size());
    assertEquals(2, nl.nodesByLabel("A").size());
    assertEquals(1, nl.nodesByLabel("B").size());
    assertEquals(ref1, nl.nodeById(1L));
    assertEquals(ref2, nl.nodeById(2L));
    assertEquals(ref3, nl.nodeById(3L));
    assertTrue(nl.nodesByLabel("A").contains(ref1));
    assertTrue(nl.nodesByLabel("A").contains(ref2));
    assertTrue(nl.nodesByLabel("B").contains(ref3));
  }

  @Test
  public void growsAboveInitialCapacity() {
    NodesList nl = new NodesList(2);

    NodeRef ref1 = dummyRef(1L, "A");
    NodeRef ref2 = dummyRef(2L, "A");
    NodeRef ref3 = dummyRef(3L, "B");
    nl.add(ref1);
    nl.add(ref2);
    nl.add(ref3);

    assertEquals(3, nl.size());
    assertEquals(2, nl.nodesByLabel("A").size());
    assertEquals(1, nl.nodesByLabel("B").size());
    assertEquals(ref1, nl.nodeById(1L));
    assertEquals(ref2, nl.nodeById(2L));
    assertEquals(ref3, nl.nodeById(3L));
    assertTrue(nl.nodesByLabel("A").contains(ref1));
    assertTrue(nl.nodesByLabel("A").contains(ref2));
    assertTrue(nl.nodesByLabel("B").contains(ref3));
  }

  @Test
  public void growsAboveInitialCapacity2() {
    NodesList nl = new NodesList(2);

    for (int i = 0; i< 50000; i++) {
      nl.add(dummyRef(i, "A" + i));
    }

    assertEquals(50000, nl.size());
  }

  // TODO test: remove nodes, reuse space

  private NodeRef dummyRef(long id, String label) {
    return new NodeRef(dummyGraph, id) {
      public String label() {
        return label;
      }
    };
  }

  private OdbGraph dummyGraph = OdbGraph.open(OdbConfig.withoutOverflow(), new ArrayList<>(), new ArrayList<>());
}
