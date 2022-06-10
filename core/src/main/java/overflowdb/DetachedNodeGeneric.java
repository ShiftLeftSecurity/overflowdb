package overflowdb;

public class DetachedNodeGeneric implements DetachedNodeData {
    private final String label;
    private Object ref;
    private Object[] propertiesAsKeyValues;
    private static Object[] emptyList = new Object[0];

    public DetachedNodeGeneric(String label, Object... propertiesAsKeyValues) {
        this.label = label;
        this.propertiesAsKeyValues = propertiesAsKeyValues.length > 0 ? propertiesAsKeyValues : emptyList;
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
    public Object[] getPropertiesAsKeyValues() {
        return propertiesAsKeyValues;
    }
}
