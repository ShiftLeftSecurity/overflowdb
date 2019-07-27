package io.shiftleft.overflowdb.structure;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.*;

//TODO MP
public class ReferenceManagerTest {

    private int heapPercentageThreshold = 80;

    @Test
    public void shouldReleaseConfiguredRefCount() {
        int releaseCount = 2;
//        ReferenceManager refMgr = new ReferenceManager(heapPercentageThreshold, releaseCount);
//        refMgr.registerRef(new DummyElementRef());
//        refMgr.clearReferences();


    }



    private class DummyElementRef extends ElementRef {
        private final String label;

        public DummyElementRef(Element element) {
            super(element.id(), element.graph(), element);
            this.label = element.label();
        }

        @Override
        protected Element readFromDisk(long elementId) throws IOException {
            throw new NotImplementedException("");
        }

        @Override
        public Property<?> property(String key, Object value) {
            throw new NotImplementedException("");
        }

        @Override
        public Iterator<? extends Property<?>> properties(String... propertyKeys) {
            throw new NotImplementedException("");
        }

        @Override
        public String label() {
            return label;
        }
    }
}
