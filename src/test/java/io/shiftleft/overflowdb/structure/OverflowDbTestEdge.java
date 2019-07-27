package io.shiftleft.overflowdb.structure;

import java.util.Arrays;
import java.util.HashSet;

public class OverflowDbTestEdge extends OverflowDbEdge {
    public static final String LABEL = "testEdge";
    public static final String LONG_PROPERTY = "longProperty";
    public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList(LONG_PROPERTY));

    public OverflowDbTestEdge(TinkerGraph graph, VertexRef<OverflowDbNode> outVertex, VertexRef<OverflowDbNode> inVertex) {
        super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
    }

    public Long longProperty() {
        return (Long) property(LONG_PROPERTY).value();
    }

    public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

    public static OverflowElementFactory.ForEdge<OverflowDbTestEdge> factory = new OverflowElementFactory.ForEdge<OverflowDbTestEdge>() {
        @Override
        public String forLabel() {
            return OverflowDbTestEdge.LABEL;
        }

        @Override
        public OverflowDbTestEdge createEdge(TinkerGraph graph, VertexRef outVertex, VertexRef inVertex) {
            return new OverflowDbTestEdge(graph, outVertex, inVertex);
        }
    };
}
