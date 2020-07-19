package overflowdb.util;

import org.junit.Test;
import overflowdb.NodeRef;
import overflowdb.OdbConfig;
import overflowdb.OdbGraph;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NodesListTest {

  @Test
  public void addElements() {
    NodesList nl = new NodesList();

    NodeRef ref1 = createDummyRef(1L, "A");
    NodeRef ref2 = createDummyRef(2L, "A");
    NodeRef ref3 = createDummyRef(3L, "B");
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

    NodeRef ref1 = createDummyRef(1L, "A");
    NodeRef ref2 = createDummyRef(2L, "A");
    NodeRef ref3 = createDummyRef(3L, "B");
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
      nl.add(createDummyRef(i, "A" + i));
    }

    assertEquals(50000, nl.size());
  }

  @Test
  public void removeNode() {
    NodesList nl = new NodesList();

    NodeRef ref1 = createDummyRef(1L, "A");
    NodeRef ref2 = createDummyRef(2L, "A");
    nl.add(ref1);
    nl.add(ref2);

    nl.remove(ref1);
    assertEquals(1, nl.size());
    assertEquals(1, nl.nodesByLabel("A").size());
    assertTrue(nl.nodesByLabel("A").contains(ref2));
    assertEquals(ref2, nl.nodeById(2L));
    assertNull(nl.nodeById(1L));
  }



  private NodeRef createDummyRef(long id, String label) {
    return new NodeRef(dummyGraph, id) {
      public String label() {
        return label;
      }
    };
  }

  private OdbGraph dummyGraph = OdbGraph.open(OdbConfig.withoutOverflow(), new ArrayList<>(), new ArrayList<>());
}
