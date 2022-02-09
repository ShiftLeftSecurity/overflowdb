package overflowdb;

public class DetachedNodeGeneric implements DetachedNodeData {
    private final String label;
    private Object ref;
    public Object[] keyvalues;
    private static Object[] emptyList = new Object[0];

    public DetachedNodeGeneric(String label, Object... keyvalues) {
        this.label = label;
        this.keyvalues = keyvalues.length > 0 ? keyvalues : emptyList;
    }

    public String label(){
        return label;
    }

    public Object getRefOrId(){
        return ref;
    }

    public void setRefOrId(Object ref){
        this.ref = ref;
    }
}
