package de.kp.works.ignite.client.query;
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

import de.kp.works.ignite.client.IgniteContext;
import de.kp.works.ignite.client.IgniteResult;

import java.util.List;

public class IgniteGetQuery extends IgniteQuery {
    /**
     * Retrieve the element (edge or vertex) that refers
     * to the provided identifier
     */
    public IgniteGetQuery(String name, IgniteContext context, Object id) {
        super(name, context);
    }
    /**
     * Retrieve all elements (edges or vertices) that refer
     * to the provided list of identifiers
     */
    public IgniteGetQuery(String name, IgniteContext context, List<Object> ids) {
        super(name, context);
    }

    @Override
    public List<IgniteResult> getResult() {
        return null;
    }
}
