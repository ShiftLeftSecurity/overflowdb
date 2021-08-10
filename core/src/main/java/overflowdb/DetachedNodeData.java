package overflowdb;

import overflowdb.BatchedUpdate;
import overflowdb.Node;

public interface DetachedNodeData extends BatchedUpdate.Change {
        public String label();
        public Node getRef();
        //setRef is not really public. It is for internal use only.
        public void setRef(Node ref);
        public boolean hasData();
        //expects String key, Object property pairs
        public void setProperties(Object... properties);
}
