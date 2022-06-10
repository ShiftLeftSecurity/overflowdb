package overflowdb;

public interface DetachedNodeData extends BatchedUpdate.Change, NodeOrDetachedNode {
        String label();
        /** RefOrId is initially null, and can be a Long if a specific id is required,
         * and is set to Node once the node has been added to the graph.
         * */
        Object getRefOrId();
        void setRefOrId(Object refOrId);
}
