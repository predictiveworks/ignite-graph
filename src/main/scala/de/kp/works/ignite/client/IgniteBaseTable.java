package de.kp.works.ignite.client;
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

import de.kp.works.ignite.client.query.IgniteGetQuery;
import de.kp.works.ignitegraph.ElementType;
import de.kp.works.ignitegraph.IgniteConstants;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

import java.util.*;

public class IgniteBaseTable {

    protected final String name;
    protected final IgniteConnect connect;

    protected final ElementType elementType;
    protected IgniteCache<String, BinaryObject> cache;

    public IgniteBaseTable(String name, IgniteConnect connect) {

        this.name = name;
        this.connect = connect;

        if (name.equals(IgniteConnect.namespace() + "_" + IgniteConstants.EDGES)) {
            elementType = ElementType.EDGE;
        }
        else if (name.equals(IgniteConnect.namespace() + "_" + IgniteConstants.VERTICES)) {
            elementType = ElementType.VERTEX;
        }
        else
            elementType = ElementType.UNDEFINED;
        /*
         * Connect to Ignite cache of the respective
         * name
         */
        cache = connect.getOrCreateCache(name);

    }

    /** METHODS TO SUPPORT BASIC CRUD OPERATIONS **/

    protected Object incrementEdge(IgniteIncrement igniteIncrement) {
       Object edgeId = igniteIncrement.getId();
        List<IgniteEdgeEntry> edge = getEdge(edgeId);
        /*
         * We expect that the respective edge exists;
         * returning `null` leads to an exception that
         * is moved to the user interface
         */
        if (edge.isEmpty()) return null;
        return incrementEdge(igniteIncrement, edge);
    }

    protected Object incrementVertex(IgniteIncrement igniteIncrement) {
        Object vertexId = igniteIncrement.getId();
        List<IgniteVertexEntry> vertex = getVertex(vertexId);
        /*
         * We expect that the respective vertex exists;
         * returning `null` leads to an exception that
         * is moved to the user interface
         */
        if (vertex.isEmpty()) return null;
        return incrementVertex(igniteIncrement, vertex);
    }

    protected void putEdge(IgnitePut ignitePut) {
        /*
         * STEP #1: Retrieve existing edge entries
         * that refer to the provided id
         */
        Object edgeId = ignitePut.getId();
        List<IgniteEdgeEntry> edge = getEdge(edgeId);

        if (edge.isEmpty())
            createEdge(ignitePut);

        else
            updateEdge(ignitePut, edge);

    }

    protected void putVertex(IgnitePut ignitePut) throws Exception {
        Object vertexId = ignitePut.getId();
        List<IgniteVertexEntry> vertex = getVertex(vertexId);

        if (vertex.isEmpty())
            createVertex(ignitePut);

        else
            updateVertex(ignitePut, vertex);
    }

    protected void deleteEdge(IgniteDelete igniteDelete) {
        Object edgeId = igniteDelete.getId();
        List<IgniteEdgeEntry> edge = getEdge(edgeId);

        if (!edge.isEmpty()) deleteEdge(igniteDelete, edge);
    }

    protected void deleteVertex(IgniteDelete igniteDelete) {
        Object vertexId = igniteDelete.getId();
        List<IgniteVertexEntry> vertex = getVertex(vertexId);

        if (!vertex.isEmpty()) deleteVertex(igniteDelete, vertex);
    }
    /**
     * Create, update & delete operation for edges
     * are manipulating operations of the respective
     * cache entries.
     *
     * Reminder: A certain edge is defined as a list
     * of (edge) cache entries
     */
    protected List<IgniteEdgeEntry> getEdge(Object id) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, connect, id);
        return igniteQuery.getEdgeEntries();
    }
    /**
     * Create, update & delete operation for vertices
     * are manipulating operations of the respective
     * cache entries.
     *
     * Reminder: A certain vertex is defined as a list
     * of (vertex) cache entries
     */
    protected List<IgniteVertexEntry> getVertex(Object id) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, connect, id);
        return igniteQuery.getVertexEntries();
    }

    // TODO ADD BASIC OPERATIONS
    /**
     * The provided [IgnitePut] is transformed into a list of
     * [IgniteEdgeEntry] and these entries are put into cache
     */
    protected void createEdge(IgnitePut ignitePut) {
        // TODO
    }

    /**
     * This method supports the modification of an existing
     * edge; this implies the update of existing property
     * values as well as the creation of new properties
     */
    protected void updateEdge(IgnitePut ignitePut, List<IgniteEdgeEntry> edge) {
        List<String> colNames = ignitePut.getColumnNames();
        // TODO
    }
    /**
     * This method supports the deletion of an entire edge
     * or just specific properties of an existing edge.
     */
    protected void deleteEdge(IgniteDelete igniteDelete, List<IgniteEdgeEntry> edge) {
        // TODO
    }

    protected Object incrementEdge(IgniteIncrement igniteIncrement, List<IgniteEdgeEntry> edge) {
        // TODO
        return null;
    }
    /**
     * The provided [IgnitePut] is transformed into a list of
     * [IgniteVertexEntry] and these entries are put into cache
     */
    protected void createVertex(IgnitePut ignitePut) throws Exception {

        String id = null;
        String idType = null;
        String label = null;
        Long createdAt = System.currentTimeMillis();
        Long updatedAt = System.currentTimeMillis();

        List<IgniteVertexEntry> entries = new ArrayList<>();
        /*
         * STEP #1: Move through all columns and
         * determine the common fields of all entries
         */
        for (IgniteColumn column : ignitePut.getColumns()) {
            switch (column.getColName()) {
                case IgniteConstants.ID_COL_NAME: {
                    id = column.getColValue().toString();
                    idType = column.getColType();
                    break;
                }
                case IgniteConstants.LABEL_COL_NAME: {
                    label = column.getColValue().toString();
                    break;
                }
                case IgniteConstants.CREATED_AT_COL_NAME: {
                    createdAt = (Long)column.getColValue();
                    break;
                }
                case IgniteConstants.UPDATED_AT_COL_NAME: {
                    updatedAt = (Long)column.getColValue();
                    break;
                }
                default:
                    break;
            }
        }
        /*
         * Check whether the core fields of a vertex entry
         * are provided
         */
        if (id == null || idType == null || label == null)
            throw new Exception("Number of parameters provided is not sufficient to creat a vertex");
        /*
         * STEP #2: Move through all property columns
         */
        for (IgniteColumn column : ignitePut.getColumns()) {
            switch (column.getColName()) {
                case IgniteConstants.ID_COL_NAME:
                case IgniteConstants.LABEL_COL_NAME:
                case IgniteConstants.CREATED_AT_COL_NAME:
                case IgniteConstants.UPDATED_AT_COL_NAME: {
                    break;
                }
                default: {
                    /*
                     * Build an entry for each property
                     */
                    String propKey   = column.getColName();
                    String propType  = column.getColType();
                    String propValue = column.getColValue().toString();
                    /*
                     * For a create request, we must generate
                     * a unique cache key for each entry
                     */
                    String cacheKey = UUID.randomUUID().toString();

                    entries.add(new IgniteVertexEntry(
                            cacheKey,
                            id,
                            idType,
                            label,
                            createdAt,
                            updatedAt,
                            propKey,
                            propType,
                            propValue));

                    break;
                }
            }
        }
        /*
         * STEP #3: Check whether the entries are still empty,
         * i.e. a vertex without properties will be created
         */
        if (entries.isEmpty()) {
            String emptyValue = "*";
            /*
             * For a create request, we must generate
             * a unique cache key for each entry
             */
            String cacheKey = UUID.randomUUID().toString();
            entries.add(new IgniteVertexEntry(cacheKey,
                    id, idType, label, createdAt, updatedAt, emptyValue, emptyValue, emptyValue));

        }
        /*
         * STEP #4: Persist all entries that describe the vertex
         * to the Ignite vertex cache
         */
        writeVertex(entries);

    }
    /**
     * This method supports the modification of an existing
     * vertex; this implies the update of existing property
     * values as well as the creation of new properties
     */
    protected void updateVertex(IgnitePut ignitePut, List<IgniteVertexEntry> vertex) {
        List<String> colNames = ignitePut.getColumnNames();
        // TODO
    }

    /**
     * This method supports the deletion of an entire vertex
     * or just specific properties of an existing vertex.
     *
     * When and entire vertex must be deleted, this methods
     * also checks whether the vertex is referenced by an edge
     */
    protected void deleteVertex(IgniteDelete igniteDelete, List<IgniteVertexEntry> vertex) {
        // TODO
    }

    protected Object incrementVertex(IgniteIncrement igniteIncrement, List<IgniteVertexEntry> vertex) {
        // TODO
        return null;
    }

    private void writeVertex(List<IgniteVertexEntry> entries) throws Exception {

        Ignite ignite = connect.getIgnite();
        Map<String,BinaryObject> row = new HashMap<>();

        for (IgniteVertexEntry entry : entries) {
            BinaryObjectBuilder valueBuilder = ignite.binary().builder(name);

            valueBuilder.setField(IgniteConstants.ID_COL_NAME,         entry.id);
            valueBuilder.setField(IgniteConstants.ID_TYPE_COL_NAME,    entry.idType);
            valueBuilder.setField(IgniteConstants.LABEL_COL_NAME,      entry.label);
            valueBuilder.setField(IgniteConstants.CREATED_AT_COL_NAME, entry.createdAt);
            valueBuilder.setField(IgniteConstants.UPDATED_AT_COL_NAME, entry.updatedAt);

            valueBuilder.setField(IgniteConstants.PROPERTY_KEY_COL_NAME,  entry.propKey);
            valueBuilder.setField(IgniteConstants.PROPERTY_TYPE_COL_NAME,  entry.propType);
            valueBuilder.setField(IgniteConstants.PROPERTY_VALUE_COL_NAME, entry.propValue);

            String cacheKey = entry.cacheKey;
            BinaryObject cacheValue = valueBuilder.build();

            row.put(cacheKey, cacheValue);
        };

        for (Map.Entry<String,BinaryObject> entry : row.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }
   }

}
