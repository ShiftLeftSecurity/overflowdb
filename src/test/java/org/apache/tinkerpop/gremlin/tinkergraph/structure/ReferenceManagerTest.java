/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

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
