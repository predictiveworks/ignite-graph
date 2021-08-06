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
import de.kp.works.ignitegraph.IgniteConstants;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class IgniteQuery {

    protected IgniteCache<String, BinaryObject> cache;
    protected String sqlStatement;

    public IgniteQuery(String name, IgniteContext context) {

        try {
            cache = context.getOrCreateCache(name);

        } catch (Exception e) {
            cache = null;
        }

    }

    public void vertexToFields(Object vertex, Direction direction, HashMap<String, String> fields) {
        /*
         * An Edge links two Vertex objects. The Direction determines
         * which Vertex is the tail Vertex (out Vertex) and which Vertex
         * is the head Vertex (in Vertex).
         *
         * [HEAD VERTEX | OUT] -- <EDGE> --> [TAIL VERTEX | IN]
         *
         * The illustration is taken from the Apache TinkerPop [Edge]
         * documentation.
         *
         * This implies: FROM = OUT & TO = IN
         */
        if (direction.equals(Direction.IN))
            fields.put(IgniteConstants.TO_COL_NAME, vertex.toString());

        else
            fields.put(IgniteConstants.FROM_COL_NAME, vertex.toString());


    }
    public abstract List<IgniteResult> getResult();

    protected abstract void createSql(String cacheName, Map<String, String> fields);

    protected List<List<?>> getSqlResult() {
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sqlStatement);
        return cache.query(sqlQuery).getAll();
    }

}
