package overflowdb;

import org.junit.Test;
import overflowdb.testdomains.simple.SimpleDomain;
import overflowdb.testdomains.simple.TestNode;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexManagerTest {

  @Test
  public void handleOldAndNewNodes() {
    try (Graph graph = SimpleDomain.newGraph()) {
      IndexManager indexManager = graph.indexManager;
      final String testValue = "test value zero";
      Node nodeCreatedBeforeIndexExisted0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, testValue);
      Node nodeCreatedBeforeIndexExisted1 = graph.addNode(TestNode.LABEL); // will set property after creating index
      Node nodeCreatedBeforeIndexExisted2 = graph.addNode(TestNode.LABEL); // will set property to something else

      indexManager.createNodePropertyIndex(TestNode.STRING_PROPERTY);
      nodeCreatedBeforeIndexExisted1.setProperty(TestNode.STRING_PROPERTY, testValue);
      nodeCreatedBeforeIndexExisted2.setProperty(TestNode.STRING_PROPERTY, "something else");
      Node nodeInsertedAfterIndexCreation = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, testValue);

      assertTrue(indexManager.isIndexed(TestNode.STRING_PROPERTY));
      assertEqualContents(Arrays.asList(
          nodeCreatedBeforeIndexExisted0,
          nodeCreatedBeforeIndexExisted1,
          nodeInsertedAfterIndexCreation
      ), indexManager.lookup(TestNode.STRING_PROPERTY, testValue));
    }
  }

  private void assertEqualContents(List expected, List actual) {
    assertEquals(expected.size(), actual.size());

    Set actualSet = new HashSet<>();
    actualSet.addAll(actual);
    assertTrue(actualSet.containsAll(expected));
  }

}
