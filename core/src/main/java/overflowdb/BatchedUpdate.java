package overflowdb;

import overflowdb.util.IteratorUtils;

import java.util.ArrayDeque;
import java.util.Iterator;

public class BatchedUpdate {

    public static final Object[] emptyArray = new Object[]{};

    public interface KeyPool{
        public long next();
    }


    public interface DiffOrBuilder {
        int size();
        Iterator<Change> iterator();
    }

    public static class DiffGraph implements DiffOrBuilder {
        public final Change[] changes;

        DiffGraph(Change[] changes) {
            this.changes = changes;
        }

        @Override public int size(){
            return changes.length;
        }
        @Override public Iterator<Change> iterator(){
            return new IteratorUtils.ArrayIterator(changes);
        }

    }

    public static class DiffGraphBuilder implements DiffOrBuilder {
        private ArrayDeque<Change> _buffer = new ArrayDeque<>();

        public DiffGraphBuilder(){}


        public DiffGraph build(){
            DiffGraph res = new DiffGraph(_buffer.toArray(new Change[]{}));
            this._buffer = null;
            return res;
        }

        public int size(){
            return _buffer.size();
        }
        public Iterator<Change> iterator(){
            return _buffer.iterator();
        }

        public DiffGraphBuilder absorb(DiffGraphBuilder other){
            if(this._buffer.size() > other._buffer.size()){
                _buffer.addAll(other._buffer);
                other._buffer = null;
            } else {
                ArrayDeque<Change> tmp = this._buffer;
                this._buffer = other._buffer;
                other._buffer = null;
                for (Iterator<Change> it = tmp.descendingIterator(); it.hasNext(); ) {
                    Change change = it.next();
                    _buffer.addFirst(change);
                }
            }
            return this;
        }

        public DiffGraphBuilder addNode(DetachedNodeData node){
            _buffer.addLast(node);
            return this;
        }

        public DiffGraphBuilder addNode(String label, Object... keyvalues){
            _buffer.addLast(new DetachedNodeGeneric(label, keyvalues));
            return this;
        }

        public DiffGraphBuilder addEdge(String label, Node src, Node dst, Object... properties){
            _buffer.addLast(new CreateEdge(label, src, dst, properties.length > 0 ? properties : null));
            return this;
        }
        public DiffGraphBuilder addEdge(String label, Node src, DetachedNodeData dst, Object... properties){
            _buffer.addLast(new CreateEdge(label, src, dst, properties.length > 0 ? properties : null));
            return this;
        }
        public DiffGraphBuilder addEdge(String label, DetachedNodeData src, Node dst, Object... properties){
            _buffer.addLast(new CreateEdge(label, src, dst, properties.length > 0 ? properties : null));
            return this;
        }
        public DiffGraphBuilder addEdge(String label, DetachedNodeData src, DetachedNodeData dst, Object... properties){
            _buffer.addLast(new CreateEdge(label, src, dst, properties.length > 0 ? properties : null));
            return this;
        }
        public DiffGraphBuilder setNodeProperty(String label, Node node, Object property){
            _buffer.addLast(new SetNodeProperty(label, node, property));
            return this;
        }
        public DiffGraphBuilder removeNode(Node node){
            _buffer.addLast(new RemoveNode(node));
            return this;
        }
        public DiffGraphBuilder removeEdge(Edge edge){
            _buffer.addLast(new RemoveEdge(edge));
            return this;
        }
        //missing API functions (not implemented because not needed at this time):
        //setEdgeProperty etc.
    }

    abstract public class ModificationListener {
        public abstract void onAfterInitNewNode(Node node);
        public abstract void onAfterAddNewEdge(Edge edge);
        public abstract void onBeforePropertyChange(Node node, String key);
        public abstract void onAfterPropertyChange(Node node, String key, Object value);
        public abstract void onBeforeRemoveNode(Node node);
        public abstract void onBeforeRemoveEdge(Edge edge);
        public abstract void finish();
    }



    public static class AppliedDiff {
       public DiffOrBuilder diffGraph;
       private ModificationListener listener;
       private int transitiveModifications;
       private Graph graph;

        AppliedDiff(Graph graph, DiffOrBuilder diffGraph, ModificationListener listener, int transitiveModifications) {
            this.graph = graph;
            this.diffGraph = diffGraph;
            this.listener = listener;
            this.transitiveModifications = transitiveModifications;
        }

        public DiffGraph getDiffGraph(){
            if(diffGraph instanceof DiffGraphBuilder){
                this.diffGraph = ((DiffGraphBuilder) diffGraph).build();
            }
            return (DiffGraph) diffGraph;
        }
        public ModificationListener getListener(){
            return listener;
        }
        public int explicitModifications(){
            return diffGraph.size();
        }
        public int transitiveModifications(){
            return transitiveModifications;
        }
    }





    //Interface Change
    public interface Change {}

    private static class RemoveEdge implements Change {
        public Edge edge;

        public RemoveEdge(Edge edge) {
            this.edge = edge;
        }
    }


    public static class CreateNode implements Change  {
        public String label;
        public Object[] ProprtiesAndKeys;
        public long id; // 0 means that the label is not unknown

        public CreateNode(String label) {
            this.label = label;
        }
    }

    public static class RemoveNode implements Change  {
        public Node node;

        public RemoveNode(Node node) {
            this.node = node;
        }
    }

    public static class CreateEdge implements Change{
        public String label;
        public Object src;
        public Object dst;
        public Object[] propertiesAndKeys;

        public CreateEdge(String label, Object src, Object dst, Object[] propertiesAndKeys) {
            this.label = label;
            this.src = src;
            this.dst = dst;
            this.propertiesAndKeys = propertiesAndKeys;
        }
    }

    public static class SetNodeProperty implements Change{
        public String label;
        public Node node;
        public Object value;

        public SetNodeProperty(String label, Node node, Object value) {
            this.label = label;
            this.node = node;
            this.value = value;
        }
    }

    public static AppliedDiff applyDiff(Graph graph, DiffOrBuilder diff){
        return new DiffGraphApplier(graph, diff, null, null).run();
    }

    public static AppliedDiff applyDiff(Graph graph, DiffOrBuilder diff, KeyPool keyPool, ModificationListener listener){
        return new DiffGraphApplier(graph, diff, keyPool, listener).run();
    }



    private static class DiffGraphApplier{
        private final DiffOrBuilder diff;
        private final KeyPool keyPool;
        private final ModificationListener listener;
        private final ArrayDeque<DetachedNodeData> deferredInitializers = new ArrayDeque<>();
        private final Graph graph;
        private int nChanges = 0;

        DiffGraphApplier(Graph graph, DiffOrBuilder diff, KeyPool keyPool, ModificationListener listener) {
            this.diff = diff;
            this.keyPool = keyPool;
            this.listener = listener;
            this.graph = graph;
        }

        AppliedDiff run(){
            try {
                for (Iterator<Change> it = diff.iterator(); it.hasNext(); ) {
                    Change change = it.next();
                    applyChange(change);
                }
            } finally {
                if(listener != null)
                    listener.finish();
            }
            return new AppliedDiff(graph, diff, listener, nChanges);
        }


        private Node mapDetached(DetachedNodeData detachedNode) {
            Object linkedNode = detachedNode.getRefOrId();
            if(linkedNode == null || linkedNode instanceof Long){
                if(linkedNode == null) {
                    if (keyPool == null) {
                        linkedNode = graph.addNode(detachedNode.label());
                    } else {
                        linkedNode = graph.addNode(keyPool.next(), detachedNode.label());
                    }
                } else if (linkedNode instanceof Long) {
                    linkedNode = graph.addNode(((Long) linkedNode).longValue(), detachedNode.label());
                }
                detachedNode.setRefOrId(linkedNode);
                deferredInitializers.addLast(detachedNode);
            }
            return (Node) linkedNode;
        }

        private void drainDeferred(){
            while(!deferredInitializers.isEmpty()){
                DetachedNodeData detachedNode = deferredInitializers.removeFirst();
                Node actualNode = (Node) detachedNode.getRefOrId();
                Node.initializeFromDetached(actualNode, detachedNode, this::mapDetached);
                nChanges += 1;
                if(listener != null){
                    listener.onAfterInitNewNode(actualNode);
                }
            }
        }

        private void applyChange(Change change){
            if(change instanceof DetachedNodeData){
                mapDetached((DetachedNodeData) change);
                drainDeferred();
            } else if(change instanceof CreateEdge){
                nChanges += 1;
                CreateEdge create = (CreateEdge) change;
                Node src = create.src instanceof DetachedNodeData ? mapDetached( (DetachedNodeData) create.src) : (Node) create.src;
                Node dst = create.dst instanceof DetachedNodeData ? mapDetached( (DetachedNodeData) create.dst) : (Node) create.dst;
                drainDeferred();
                Object[] properties = create.propertiesAndKeys == null ? emptyArray : create.propertiesAndKeys;
                if(listener != null){
                    Edge edge = src.addEdge(create.label, dst, properties);
                    listener.onAfterAddNewEdge(edge);
                } else {
                    src.addEdgeSilent(create.label, dst, properties);
                }
            } else if (change instanceof RemoveEdge){
                nChanges += 1;
                RemoveEdge remove = (RemoveEdge) change;
                if(listener != null)
                    listener.onBeforeRemoveEdge(remove.edge);
                remove.edge.remove();
            } else if(change instanceof RemoveNode){
                nChanges += 1;
                RemoveNode remove = (RemoveNode) change;
                if(listener != null) {
                    listener.onBeforeRemoveNode(remove.node);
                }
            } else if (change instanceof SetNodeProperty){
                nChanges += 1;
                SetNodeProperty setProp = (SetNodeProperty) change;
                if(listener != null)
                    listener.onBeforePropertyChange(setProp.node, setProp.label);
                setProp.node.setProperty(setProp.label, setProp.value);
                if(listener != null)
                    listener.onAfterPropertyChange(setProp.node, setProp.label, setProp.value);
                drainDeferred();
            }
        }
    }



}
