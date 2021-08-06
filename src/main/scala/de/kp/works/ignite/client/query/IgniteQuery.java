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

import de.kp.works.ignite.client.*;
import de.kp.works.ignitegraph.IgniteConstants;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class IgniteQuery {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteQuery.class);

    protected IgniteCache<String, BinaryObject> cache;
    protected String sqlStatement;

    public IgniteQuery(String name, IgniteContext context) {

        try {
            cache = context.getOrCreateCache(name);

        } catch (Exception e) {
            cache = null;
        }

    }

    protected void vertexToFields(Object vertex, Direction direction, HashMap<String, String> fields) {
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
    public List<IgniteResult> getResult() {

        List<IgniteResult> result = new ArrayList<>();
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            return result;

        List<List<?>> sqlResult = getSqlResult();
        try {
            if (cache == null)
                throw new Exception("Cache is not initialized.");

            String cacheName = cache.getName();
            if (cacheName.equals(IgniteContext.namespace + "_" + IgniteConstants.EDGES)) {
                List<IgniteEdgeEntry> entries = parseEdges(sqlResult);

                // TODO :: Group entries into edge rows
            }
            else if (cacheName.equals(IgniteContext.namespace + "_" + IgniteConstants.VERTICES)) {
                List<IgniteVertexEntry> entries = parseVertices(sqlResult);

                // TODO :: Group entries into edge rows

            }
            else
                throw new Exception("Cache '" + cacheName +  "' is not supported.");

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);
        }
        return result;

    }

    private List<IgniteEdgeEntry> parseEdges(List<List<?>> sqlResult) {
        // TODO
        return null;
    }

    private List<IgniteVertexEntry> parseVertices(List<List<?>> sqlResult) {
        // TODO
        return null;
    }

    protected abstract void createSql(Map<String, String> fields);

    protected List<List<?>> getSqlResult() {
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sqlStatement);
        return cache.query(sqlQuery).getAll();
    }

    protected void buildSelectPart() throws Exception {

        if (cache == null)
            throw new Exception("Cache is not initialized.");

        List<String> columns = getColumns();

        sqlStatement = "select";

        sqlStatement += " " + String.join(",", columns);
        sqlStatement += " from " + cache.getName();

    }
    /**
     * This method retrieves the `select` columns
     * of the respective cache.
     */
    protected List<String> getColumns() throws Exception {

        List<String> columns = new ArrayList<>();
        if (cache == null)
            throw new Exception("Cache is not initialized.");

        String cacheName = cache.getName();
        if (cacheName.equals(IgniteContext.namespace + "_" + IgniteConstants.EDGES)) {
            /*
             * The edge identifier used by TinkerPop to
             * identify an equivalent of a data row
             */
            columns.add(IgniteConstants.ID_COL_NAME);
            /*
             * The edge identifier type to reconstruct the
             * respective value. IgniteGraph supports [Long]
             * as well as [String] as identifier.
             */
            columns.add(IgniteConstants.ID_TYPE_COL_NAME);
            /*
             * The edge label used by TinkerPop and IgniteGraph
             */
            columns.add(IgniteConstants.LABEL_COL_NAME);
            /*
             * The `TO` vertex description
             */
            columns.add(IgniteConstants.TO_COL_NAME);
            columns.add(IgniteConstants.TO_TYPE_COL_NAME);
            /*
             * The `FROM` vertex description
             */
            columns.add(IgniteConstants.FROM_COL_NAME);
            columns.add(IgniteConstants.FROM_TYPE_COL_NAME);
            /*
             * The timestamp this cache entry has been created.
             */
            columns.add(IgniteConstants.CREATED_AT_COL_NAME);
            /*
             * The timestamp this cache entry has been updated.
             */
            columns.add(IgniteConstants.UPDATED_AT_COL_NAME);
            /*
             * The property section of this cache entry
             */
            columns.add(IgniteConstants.PROPERTY_KEY_COL_NAME);
            columns.add(IgniteConstants.PROPERTY_TYPE_COL_NAME);
            /*
             * The serialized property value
             */
            columns.add(IgniteConstants.PROPERTY_VALUE_COL_NAME);
            /*
             * The [ByteBuffer] representation for the
             * property value is an internal field and
             * not exposed to queries
             */
            return columns;
        }
        if (cacheName.equals(IgniteContext.namespace + "_" + IgniteConstants.VERTICES)) {
            /*
             * The vertex identifier used by TinkerPop to identify
             * an equivalent of a data row
             */
            columns.add(IgniteConstants.ID_COL_NAME);
            /*
             * The vertex identifier type to reconstruct the
             * respective value. IgniteGraph supports [Long]
             * as well as [String] as identifier.
             */
            columns.add(IgniteConstants.ID_TYPE_COL_NAME);
            /*
             * The vertex label used by TinkerPop and IgniteGraph
             */
            columns.add(IgniteConstants.LABEL_COL_NAME);
            /*
             * The timestamp this cache entry has been created.
             */
            columns.add(IgniteConstants.CREATED_AT_COL_NAME);
            /*
             * The timestamp this cache entry has been updated.
             */
            columns.add(IgniteConstants.UPDATED_AT_COL_NAME);
            /*
             * The property section of this cache entry
             */
            columns.add(IgniteConstants.PROPERTY_KEY_COL_NAME);
            columns.add(IgniteConstants.PROPERTY_TYPE_COL_NAME);
            /*
             * The serialized property value
             */
            columns.add(IgniteConstants.PROPERTY_VALUE_COL_NAME);
            /*
             * The [ByteBuffer] representation for the
             * property value is an internal field and
             * not exposed to queries
             */
            return columns;
        }
        throw new Exception("Cache '" + cacheName +  "' is not supported.");

    }
}
