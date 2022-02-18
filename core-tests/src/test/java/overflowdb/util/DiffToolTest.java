package overflowdb.util;

import org.junit.Test;
import overflowdb.Graph;
import overflowdb.Node;
import overflowdb.testdomains.simple.SimpleDomain;
import overflowdb.testdomains.simple.TestEdge;
import overflowdb.testdomains.simple.TestNode;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class DiffToolTest {

    @Test
    public void returnEmptyListForTheSameGraph() {
        Graph graph0 = SimpleDomain.newGraph();
        Node g0n0 = graph0.addNode(TestNode.LABEL, TestNode.INT_PROPERTY, 0);
        Node g0n1 = graph0.addNode(TestNode.LABEL, TestNode.INT_PROPERTY, 1);
        g0n0.addEdge(TestEdge.LABEL, g0n1, TestEdge.LONG_PROPERTY, 1l);

        List<String> diffRes = DiffTool.compare(graph0, graph0);

        assertTrue(diffRes.isEmpty());
    }

    @Test
    public void returnEmptyListForTheCopyOfTheGraph() {
        Graph graph0 = SimpleDomain.newGraph();
        Node g0n0 = graph0.addNode(TestNode.LABEL, TestNode.INT_PROPERTY, 0);
        Node g0n1 = graph0.addNode(TestNode.LABEL, TestNode.INT_PROPERTY, 1);
        g0n0.addEdge(TestEdge.LABEL, g0n1, TestEdge.LONG_PROPERTY, 1l);

        Graph graph1 = SimpleDomain.newGraph();
        Node g1n0 = graph1.addNode(TestNode.LABEL, TestNode.INT_PROPERTY, 0);
        Node g1n1 = graph1.addNode(TestNode.LABEL, TestNode.INT_PROPERTY, 1);
        g1n0.addEdge(TestEdge.LABEL, g1n1, TestEdge.LONG_PROPERTY, 1l);

        List<String> diffRes = DiffTool.compare(graph0, graph1);
        assertTrue(diffRes.isEmpty());

        // TODO: commutative property would be a good candidate for property based testing
        List<String> diffRes2 = DiffTool.compare(graph1, graph0);
        assertTrue(diffRes2.isEmpty());
    }

    @Test
    public void returnSomeDifferencesIfOneGraphHasMoreNodeProperties() {
        Graph graph0 = SimpleDomain.newGraph();
        Node g0n0 = graph0.addNode(TestNode.LABEL, TestNode.INT_PROPERTY, 0);
        Node g0n1 = graph0.addNode(TestNode.LABEL, TestNode.INT_PROPERTY, 1);
        g0n0.addEdge(TestEdge.LABEL, g0n1, TestEdge.LONG_PROPERTY, 1l);

        Graph graph1 = SimpleDomain.newGraph();
        Node g1n0 = graph1.addNode(TestNode.LABEL);
        Node g1n1 = graph1.addNode(TestNode.LABEL);
        g1n0.addEdge(TestEdge.LABEL, g1n1, TestEdge.LONG_PROPERTY, 1l);

        List<String> diffRes = DiffTool.compare(graph0, graph1);
        assertTrue(!diffRes.isEmpty());

        List<String> diffRes2 = DiffTool.compare(graph1, graph0);

        assertTrue(!diffRes2.isEmpty());
        // TODO: ideally we would diffRes == diffRes2 but right not it will not work as the returned strings are different
        assertTrue(diffRes.size() == diffRes2.size());
    }

    @Test
    public void returnSomeDifferencesIfOneGraphHasMoreEdges() {
        Graph graph0 = SimpleDomain.newGraph();
        Node g0n0 = graph0.addNode(TestNode.LABEL);
        Node g0n1 = graph0.addNode(TestNode.LABEL);
        g0n0.addEdge(TestEdge.LABEL, g0n1, TestEdge.LONG_PROPERTY, 1l);

        Graph graph1 = SimpleDomain.newGraph();
        graph1.addNode(TestNode.LABEL);
        graph1.addNode(TestNode.LABEL);

        List<String> diffRes = DiffTool.compare(graph0, graph1);
        assertTrue(!diffRes.isEmpty());

        List<String> diffRes2 = DiffTool.compare(graph1, graph0);

        assertTrue(!diffRes2.isEmpty());
        // TODO: ideally we would diffRes == diffRes2 but right not it will not work as the returned strings are different
        assertTrue(diffRes.size() == diffRes2.size());
    }

}
