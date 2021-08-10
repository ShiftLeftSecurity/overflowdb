package overflowdb;

public class DetachedNodeNoData implements DetachedNodeData {
    private final String label;
    private Object ref;

    public DetachedNodeNoData(String label) {
        this.label = label;
    }

    public boolean hasData(){
        return false;
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

    @Override
    public void setProperty(String key, Object value) {
        throw new RuntimeException("No support for generic SetProperty!");
    }
}
