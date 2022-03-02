package overflowdb;

import overflowdb.BatchedUpdate;
import overflowdb.Node;

public interface DetachedNodeData extends BatchedUpdate.Change, NodeOrDetachedNode {
        public String label();
        /** RefOrId is initially null, and can be a Long if a specific id is required,
         * and is set to Node once the node has been added to the graph.
         * */
        public Object getRefOrId();
        public void setRefOrId(Object refOrId);
}
