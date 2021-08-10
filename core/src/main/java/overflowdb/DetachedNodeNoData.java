package overflowdb;

public class DetachedNodeNoData implements DetachedNodeData {
    private final String label;
    private Node ref;

    public DetachedNodeNoData(String label) {
        this.label = label;
    }

    public boolean hasData(){
        return false;
    }

    public String label(){
        return label;
    }

    public Node getRef(){
        return ref;
    }

    public void setRef(Node ref){
        this.ref = ref;
    }

    @Override
    public void setProperties(Object... properties) {
        throw new RuntimeException("No support for generic SetProperty!");
    }
}
