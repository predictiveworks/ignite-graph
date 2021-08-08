package de.kp.works.ignitegraph.mutators;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.ignite.client.IgniteDelete;
import de.kp.works.ignite.client.IgniteMutation;
import de.kp.works.ignite.client.IgnitePut;
import de.kp.works.ignitegraph.*;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class PropertyRemover implements Mutator {

    private final Element element;
    private final String key;

    public PropertyRemover(IgniteGraph graph, Element element, String key) {
        this.element = element;
        this.key = key;
    }

    @Override
    public Iterator<IgniteMutation> constructMutations() {

        Object id = element.id();

        IgniteDelete delete = new IgniteDelete(id);
        delete.addColumn(key);

        IgnitePut put = new IgnitePut(id);
        put.addColumn(IgniteConstants.ID_COL_NAME, ValueUtils.getValueType(id).name(),
                id.toString());

        Long updatedAt = ((IgniteElement) element).updatedAt();
        put.addColumn(IgniteConstants.UPDATED_AT_COL_NAME, IgniteConstants.LONG_COL_TYPE,
                updatedAt.toString());

        return IteratorUtils.of(delete, put);
    }
}
