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

import de.kp.works.ignite.client.query.IgniteEdgesExistQuery;
import de.kp.works.ignite.client.query.IgniteGetQuery;
import de.kp.works.ignitegraph.ElementType;
import de.kp.works.ignitegraph.IgniteConstants;
import de.kp.works.ignitegraph.ValueType;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

import java.util.*;
import java.util.stream.Collectors;

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

    protected Object incrementEdge(IgniteIncrement igniteIncrement) throws Exception {
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

    protected Object incrementVertex(IgniteIncrement igniteIncrement) throws Exception {
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

    protected void putEdge(IgnitePut ignitePut) throws Exception {
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

    protected void deleteVertex(IgniteDelete igniteDelete) throws Exception {
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
    /**
     * Check whether a vertex is referenced by edges
     * either as `from` or `to` vertex
     */
    protected boolean hasEdges(Object vertex) {
        IgniteEdgesExistQuery igniteQuery = new IgniteEdgesExistQuery(name, connect, vertex);
        List<IgniteEdgeEntry> edges = igniteQuery.getEdgeEntries();

        return !edges.isEmpty();
    }
    /**
     * The provided [IgnitePut] is transformed into a list of
     * [IgniteEdgeEntry] and these entries are put into cache
     */
    protected void createEdge(IgnitePut ignitePut) throws Exception {

        String id         = null;
        String idType     = null;
        String label      = null;
        String toId       = null;
        String toIdType   = null;
        String fromId     = null;
        String fromIdType = null;

        Long createdAt = System.currentTimeMillis();
        Long updatedAt = System.currentTimeMillis();

        List<IgniteEdgeEntry> entries = new ArrayList<>();
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
                case IgniteConstants.TO_COL_NAME: {
                    toId = column.getColValue().toString();
                    toIdType = column.getColType();
                    break;
                }
                case IgniteConstants.FROM_COL_NAME: {
                    fromId = column.getColValue().toString();
                    fromIdType = column.getColType();
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
         * Check whether the core fields of an edge entry
         * are provided
         */
        if (id == null || idType == null || label == null || toId == null || toIdType == null || fromId == null || fromIdType == null)
            throw new Exception("Number of parameters provided is not sufficient to create an edge.");

        /*
         * STEP #2: Move through all property columns
         */
        for (IgniteColumn column : ignitePut.getColumns()) {
            switch (column.getColName()) {
                case IgniteConstants.ID_COL_NAME:
                case IgniteConstants.LABEL_COL_NAME:
                case IgniteConstants.TO_COL_NAME:
                case IgniteConstants.FROM_COL_NAME:
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

                    entries.add(new IgniteEdgeEntry(
                            cacheKey,
                            id,
                            idType,
                            label,
                            toId,
                            toIdType,
                            fromId,
                            fromIdType,
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
            entries.add(new IgniteEdgeEntry(cacheKey,
                    id, idType, label, toId, toIdType, fromId, fromIdType,
                    createdAt, updatedAt, emptyValue, emptyValue, emptyValue));

        }
        /*
         * STEP #4: Persist all entries that describe the edge
         * to the Ignite edge cache
         */
        writeEdge(entries);
    }

    /**
     * This method supports the modification of an existing
     * edge; this implies the update of existing property
     * values as well as the creation of new properties
     *
     * TODO ::
     * The current implementation does not support any
     * transactions to ensure consistency.
     */
    protected void updateEdge(IgnitePut ignitePut, List<IgniteEdgeEntry> edge) throws Exception {
        /*
         * STEP #1: Retrieve all properties that are
         * provided
         */
        List<IgniteColumn> properties = ignitePut.getProperties()
                .collect(Collectors.toList());
        /*
         * STEP #2: Distinguish between those properties
         * that are edge properties already and determine
         * those that do not exist
         */
        List<IgniteColumn> knownProps = new ArrayList<>();
        List<IgniteColumn> unknownProps = new ArrayList<>();

        for (IgniteColumn property : properties) {
            String propKey = property.getColName();
            if (edge.stream().anyMatch(entry -> entry.propKey.equals(propKey)))
                knownProps.add(property);

            else
                unknownProps.add(property);
        }
        /*
         * STEP #3: Update known properties
         */
        List<IgniteEdgeEntry> updatedEntries = edge.stream()
                .map(entry -> {
                    String propKey = entry.propKey;
                    /*
                     * Determine provided values that matches
                     * the property key of the entry
                     */
                    IgniteColumn property = knownProps.stream()
                            .filter(p -> p.getColName().equals(propKey)).collect(Collectors.toList()).get(0);

                    Object newValue = property.getColValue();
                    return new IgniteEdgeEntry(
                            entry.cacheKey,
                            entry.id,
                            entry.idType,
                            entry.label,
                            entry.toId,
                            entry.toIdType,
                            entry.fromId,
                            entry.fromIdType,
                            entry.createdAt,
                            /*
                             * Update the entry's `updatedAt` timestamp
                             */
                            System.currentTimeMillis(),
                            entry.propKey,
                            entry.propType,
                            newValue.toString());

                })
                .collect(Collectors.toList());

        writeEdge(updatedEntries);
        /*
         * STEP #4: Add unknown properties; note, we use the first
         * edge entry as a template for the common parameters
         */
        IgniteEdgeEntry template = edge.get(0);
        List<IgniteEdgeEntry> newEntries = unknownProps.stream()
                .map(property -> {
                    /*
                     * For a create request, we must generate
                     * a unique cache key for each entry
                     */
                    String cacheKey = UUID.randomUUID().toString();
                    return new IgniteEdgeEntry(
                            cacheKey,
                            template.id,
                            template.idType,
                            template.label,
                            template.toId,
                            template.toIdType,
                            template.fromId,
                            template.fromIdType,
                            System.currentTimeMillis(),
                            System.currentTimeMillis(),
                            property.getColName(),
                            property.getColType(),
                            property.getColValue().toString());
                })
                .collect(Collectors.toList());

        writeEdge(newEntries);
    }
    /**
     * This method supports the deletion of an entire edge
     * or just specific properties of an existing edge.
     */
    protected void deleteEdge(IgniteDelete igniteDelete, List<IgniteEdgeEntry> edge) {

        List<String> cacheKeys;

        List<IgniteColumn> columns = igniteDelete.getColumns();
        /*
         * STEP #1: Check whether we must delete the
         * entire edge or just a certain column
         */
        if (columns.isEmpty()) {
            /*
             * All cache entries that refer to the specific
             * edge must be deleted.
             */
            cacheKeys = edge.stream()
                    .map(entry -> entry.cacheKey).collect(Collectors.toList());
        }
        else {
            /*
             * All cache entries that refer to a certain
             * property key must be deleted
             */
            List<String> propKeys = igniteDelete.getProperties()
                    .map(c -> c.getColValue().toString())
                    .collect(Collectors.toList());

            cacheKeys = edge.stream()
                    /*
                     * Restrict to those cache entries that refer
                     * to the provided property keys
                     */
                    .filter(entry -> propKeys.contains(entry.propKey))
                    .map(entry -> entry.cacheKey).collect(Collectors.toList());

        }

        if (!cacheKeys.isEmpty())
            cache.removeAll(new HashSet<>(cacheKeys));
    }
    /**
     * This method increments a certain property value
     */
    protected Object incrementEdge(IgniteIncrement igniteIncrement, List<IgniteEdgeEntry> edge) throws Exception {
        IgniteColumn column = igniteIncrement.getColumn();
        if (column == null) return null;
        /*
         * Check whether the column value is a [Long]
         */
        String colType = column.getColType();
        if (!colType.equals(ValueType.LONG.name()))
            return null;
        /*
         * Restrict to that edge entry that refer to the
         * provided column
         */
        String colName = column.getColName();
        String colValue = column.getColValue().toString();

        List<IgniteEdgeEntry> entries = edge.stream()
                .filter(entry -> entry.propKey.equals(colName) && entry.propType.equals(colType) && entry.propValue.equals(colValue))
                .collect(Collectors.toList());

        if (entries.isEmpty()) return null;
        IgniteEdgeEntry entry = entries.get(0);

        long oldValue = Long.parseLong(entry.propValue);
        Long newValue = oldValue + 1;

        IgniteEdgeEntry newEntry = new IgniteEdgeEntry(
                entry.cacheKey,
                entry.id,
                entry.idType,
                entry.label,
                entry.toId,
                entry.toIdType,
                entry.fromId,
                entry.fromIdType,
                entry.createdAt,
                System.currentTimeMillis(),
                entry.propKey,
                entry.propType,
                newValue.toString());

        writeEdge(Collections.singletonList(newEntry));
        return newValue;
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
            throw new Exception("Number of parameters provided is not sufficient to create a vertex.");
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
     *
     * TODO ::
     * The current implementation does not support any
     * transactions to ensure consistency.
     */
    protected void updateVertex(IgnitePut ignitePut, List<IgniteVertexEntry> vertex) throws Exception {
        /*
         * STEP #1: Retrieve all properties that are
         * provided
         */
        List<IgniteColumn> properties = ignitePut.getProperties()
                .collect(Collectors.toList());
        /*
         * STEP #2: Distinguish between those properties
         * that are edge properties already and determine
         * those that do not exist
         */
        List<IgniteColumn> knownProps = new ArrayList<>();
        List<IgniteColumn> unknownProps = new ArrayList<>();

        for (IgniteColumn property : properties) {
            String propKey = property.getColName();
            if (vertex.stream().anyMatch(entry -> entry.propKey.equals(propKey)))
                knownProps.add(property);

            else
                unknownProps.add(property);
        }
        /*
         * STEP #3: Update known properties
         */
        List<IgniteVertexEntry> updatedEntries = vertex.stream()
                .map(entry -> {
                    String propKey = entry.propKey;
                    /*
                     * Determine provided values that matches
                     * the property key of the entry
                     */
                    IgniteColumn property = knownProps.stream()
                            .filter(p -> p.getColName().equals(propKey)).collect(Collectors.toList()).get(0);

                    Object newValue = property.getColValue();
                    return new IgniteVertexEntry(
                            entry.cacheKey,
                            entry.id,
                            entry.idType,
                            entry.label,
                            entry.createdAt,
                            /*
                             * Update the entry's `updatedAt` timestamp
                             */
                            System.currentTimeMillis(),
                            entry.propKey,
                            entry.propType,
                            newValue.toString());

                })
                .collect(Collectors.toList());

        writeVertex(updatedEntries);
        /*
         * STEP #4: Add unknown properties; note, we use the first
         * vertex entry as a template for the common parameters
         */
        IgniteVertexEntry template = vertex.get(0);
        List<IgniteVertexEntry> newEntries = unknownProps.stream()
                .map(property -> {
                    /*
                     * For a create request, we must generate
                     * a unique cache key for each entry
                     */
                    String cacheKey = UUID.randomUUID().toString();
                    return new IgniteVertexEntry(
                            cacheKey,
                            template.id,
                            template.idType,
                            template.label,
                            System.currentTimeMillis(),
                            System.currentTimeMillis(),
                            property.getColName(),
                            property.getColType(),
                            property.getColValue().toString());
                })
                .collect(Collectors.toList());

        writeVertex(newEntries);
    }
    /**
     * This method supports the deletion of an entire vertex
     * or just specific properties of an existing vertex.
     *
     * When and entire vertex must be deleted, this methods
     * also checks whether the vertex is referenced by an edge
     */
    protected void deleteVertex(IgniteDelete igniteDelete, List<IgniteVertexEntry> vertex) throws Exception {

        List<String> cacheKeys;

        List<IgniteColumn> columns = igniteDelete.getColumns();
        /*
         * STEP #1: Check whether we must delete the
         * entire vertex or just a certain column
         */
        if (columns.isEmpty()) {
            /*
             * All cache entries that refer to the specific
             * vertex must be deleted.
             */
            Object id = igniteDelete.getId();
            if (hasEdges(id))
                throw new Exception("The vertex '" + id.toString() + "' is referenced by at least one edge.");

            cacheKeys = vertex.stream()
                    .map(entry -> entry.cacheKey).collect(Collectors.toList());
        }
        else {
            /*
             * All cache entries that refer to a certain
             * property key must be deleted
             */
            List<String> propKeys = igniteDelete.getProperties()
                    .map(IgniteColumn::getColName)
                    .collect(Collectors.toList());

            cacheKeys = vertex.stream()
                    /*
                     * Restrict to those cache entries that refer
                     * to the provided property keys
                     */
                    .filter(entry -> propKeys.contains(entry.propKey))
                    .map(entry -> entry.cacheKey).collect(Collectors.toList());

        }

        cache.removeAll(new HashSet<>(cacheKeys));
    }

    protected Object incrementVertex(IgniteIncrement igniteIncrement, List<IgniteVertexEntry> vertex) throws Exception {
        IgniteColumn column = igniteIncrement.getColumn();
        if (column == null) return null;
        /*
         * Check whether the column value is a [Long]
         */
        String colType = column.getColType();
        if (!colType.equals(ValueType.LONG.name()))
            return null;
        /*
         * Restrict to that vertex entry that refer to the
         * provided column
         */
        String colName = column.getColName();
        String colValue = column.getColValue().toString();

        List<IgniteVertexEntry> entries = vertex.stream()
                .filter(entry -> entry.propKey.equals(colName) && entry.propType.equals(colType) && entry.propValue.equals(colValue))
                .collect(Collectors.toList());

        if (entries.isEmpty()) return null;
        IgniteVertexEntry entry = entries.get(0);

        long oldValue = Long.parseLong(entry.propValue);
        Long newValue = oldValue + 1;

        IgniteVertexEntry newEntry = new IgniteVertexEntry(
                entry.cacheKey,
                entry.id,
                entry.idType,
                entry.label,
                entry.createdAt,
                System.currentTimeMillis(),
                entry.propKey,
                entry.propType,
                newValue.toString());

        writeVertex(Collections.singletonList(newEntry));
        return newValue;
    }

    /**
     * Supports create and update operations for edges
     */
    private void writeEdge(List<IgniteEdgeEntry> entries) throws Exception {

        Ignite ignite = connect.getIgnite();
        Map<String,BinaryObject> row = new HashMap<>();

        for (IgniteEdgeEntry entry : entries) {
            BinaryObjectBuilder valueBuilder = ignite.binary().builder(name);

            valueBuilder.setField(IgniteConstants.ID_COL_NAME,         entry.id);
            valueBuilder.setField(IgniteConstants.ID_TYPE_COL_NAME,    entry.idType);
            valueBuilder.setField(IgniteConstants.LABEL_COL_NAME,      entry.label);

            valueBuilder.setField(IgniteConstants.TO_COL_NAME,      entry.toId);
            valueBuilder.setField(IgniteConstants.TO_TYPE_COL_NAME, entry.toIdType);

            valueBuilder.setField(IgniteConstants.FROM_COL_NAME,      entry.fromId);
            valueBuilder.setField(IgniteConstants.FROM_TYPE_COL_NAME, entry.fromIdType);

            valueBuilder.setField(IgniteConstants.CREATED_AT_COL_NAME, entry.createdAt);
            valueBuilder.setField(IgniteConstants.UPDATED_AT_COL_NAME, entry.updatedAt);

            valueBuilder.setField(IgniteConstants.PROPERTY_KEY_COL_NAME,  entry.propKey);
            valueBuilder.setField(IgniteConstants.PROPERTY_TYPE_COL_NAME,  entry.propType);
            valueBuilder.setField(IgniteConstants.PROPERTY_VALUE_COL_NAME, entry.propValue);

            String cacheKey = entry.cacheKey;
            BinaryObject cacheValue = valueBuilder.build();

            row.put(cacheKey, cacheValue);
        }

        for (Map.Entry<String,BinaryObject> entry : row.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }
    }
    /**
     * Supports create and update operations for vertices
     */
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
        }

        for (Map.Entry<String,BinaryObject> entry : row.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }
   }

}
