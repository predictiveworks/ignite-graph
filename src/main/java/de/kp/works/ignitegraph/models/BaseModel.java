package de.kp.works.ignitegraph.models;
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

import de.kp.works.ignite.IgniteTable;
import de.kp.works.ignitegraph.IgniteGraph;

public abstract class BaseModel {

    protected final IgniteGraph graph;
    protected final IgniteTable table;

    public BaseModel(IgniteGraph graph, IgniteTable table) {
        this.graph = graph;
        this.table = table;
    }

    public IgniteGraph getGraph() {
        return graph;
    }

    public IgniteTable getTable() {
        return table;
    }

}