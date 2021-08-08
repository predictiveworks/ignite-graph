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

import de.kp.works.ignite.client.query.*;
import de.kp.works.ignitegraph.ElementType;
import de.kp.works.ignitegraph.IgniteConstants;
import de.kp.works.ignitegraph.IgniteVertex;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.stream.Collectors;

public class IgniteTable extends IgniteBaseTable {

    public IgniteTable(String name, IgniteConnect connect) {
        super(name, connect);
    }
    /**
     * This method adds or updates an Ignite cache entry;
     * note, the current implementation requires a fully
     * qualified cache entry.
     */
    public boolean put(IgnitePut ignitePut) throws Exception {

        if (connect == null) return false;
        if (connect.getIgnite() == null) return false;

        try {

            if (elementType.equals(ElementType.EDGE)) {
                putEdge(ignitePut);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                putVertex(ignitePut);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported.");

            return true;

        } catch (Exception e) {
            return false;
        }

    }
    /**
     * Delete supports deletion of an entire element
     * (edge or vertex) and also the removal of a certain
     * property.
     */
    public boolean delete(IgniteDelete igniteDelete) throws Exception {

        if (connect == null) return false;
        if (connect.getIgnite() == null) return false;

        try {

            if (elementType.equals(ElementType.EDGE)) {
                deleteEdge(igniteDelete);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                deleteVertex(igniteDelete);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported.");

             return true;

        } catch (Exception e) {
            return false;
        }
    }

    public Object increment(IgniteIncrement igniteIncrement) throws Exception {

        if (connect == null) return false;
        if (connect.getIgnite() == null) return false;
        /*
         * In case of an increment, the respective incremented
         * value is returned,
         */
        try {

            if (elementType.equals(ElementType.EDGE)) {
                return incrementEdge(igniteIncrement);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                return incrementVertex(igniteIncrement);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported.");


        } catch (Exception e) {
            return null;
        }

    }

    public void batch(List<IgniteMutation> mutations, Object[] results) throws Exception {

         for (int i = 0; i < mutations.size(); i++) {
             /*
              * Determine the respective mutation
              */
             IgniteMutation mutation = mutations.get(i);
             if (mutation.mutationType.equals(IgniteMutationType.DELETE)) {

                 IgniteDelete deleteMutation = (IgniteDelete)mutation;

                 boolean success = delete(deleteMutation);
                 if (!success) {
                     /*
                      * See [Mutators]: In case of a failed delete
                      * operation an exception is returned
                      */
                     results[i] = new Exception(
                         "Deletion of element '" + deleteMutation.getId().toString() + "' failed in table '" + name + "'.");

                 }
                 else
                     results[i] = deleteMutation.getId();

             }
             else if (mutation.mutationType.equals(IgniteMutationType.INCREMENT)) {

                 IgniteIncrement incrementMutation = (IgniteIncrement)mutation;

                 Object value = increment(incrementMutation);
                 if (value == null) {
                     /*
                      * See [Mutators]: In case of a failed delete
                      * operation an exception is returned
                      */
                     results[i] = new Exception(
                             "Increment of element '" + incrementMutation.getId().toString() + "' failed in table '" + name + "'.");

                 }
                 else
                     results[i] = value;

             }
             else {

                 IgnitePut putMutation = (IgnitePut)mutation;

                 boolean success = put(putMutation);
                 if (!success) {
                     /*
                      * See [Mutators]: In case of a failed delete
                      * operation an exception is returned
                      */
                     results[i] = new Exception(
                             "Deletion of element '" + putMutation.getId().toString() + "' failed in table '" + name + "'.");

                 }
                 else
                     results[i] = putMutation.getId();
                 // TODO

             }

        }
    }

    private Map<String,BinaryObject> buildRow(IgnitePut put) throws Exception {

        if (elementType.equals(ElementType.EDGE)) {
            return buildEdgeRow(put);
        }
        if (elementType.equals(ElementType.VERTEX)) {
            return buildVertexRow(put);
        }
        throw new Exception("Table '" + name +  "' is not supported.");

    }

    private Map<String, BinaryObject> buildEdgeRow(IgnitePut put) throws Exception {

        Map<String, BinaryObject> row = new HashMap<>();

        List<IgniteColumn> columns = put.getColumns();
        List<IgniteColumn> attributes = columns.stream()
                .filter(column -> !column.getColName()
                        .equals(IgniteConstants.PROPERTY_KEY_COL_NAME))
                .collect(Collectors.toList());

        /*
         * Check how many properties must be set to the cache;
         * Each property is described as an individual entry.
         */
        List<IgniteColumn> properties = columns.stream()
                .filter(column -> column.getColName()
                        .equals(IgniteConstants.PROPERTY_KEY_COL_NAME))
                .collect(Collectors.toList());

        if (properties.isEmpty()) {
            /*
             * There are not properties assigned to the edge;
             * in this case, a single cache entry is created
             * with a dummy (or empty) property.
             */
            BinaryObjectBuilder valueBuilder = buildEdgeBuilder(attributes);
            /*
             * An empty property is added
             */
            String emptyValue = "*";

            valueBuilder.setField(IgniteConstants.PROPERTY_KEY_COL_NAME, emptyValue);
            valueBuilder.setField(IgniteConstants.PROPERTY_TYPE_COL_NAME, emptyValue);

            valueBuilder.setField(IgniteConstants.PROPERTY_VALUE_COL_NAME, emptyValue);

            ByteBuffer emptyBuffer = ByteBuffer.wrap(emptyValue.getBytes());
            valueBuilder.setField(IgniteConstants.BYTE_BUFFER_COL_NAME, emptyBuffer);
            /*
             * Build cache entry key
             */
            List<String> keys = new ArrayList<>(buildAttributeParts(attributes));

            List<String> propertyParts = new ArrayList<>();
            propertyParts.add(IgniteConstants.PROPERTY_KEY_COL_NAME + "::" + emptyValue);
            propertyParts.add(IgniteConstants.PROPERTY_VALUE_COL_NAME + "::" + emptyValue);

            keys.addAll(propertyParts);
            String cacheKey = Arrays.toString(MessageDigest.getInstance("MD5")
                    .digest(String.join("#", keys)
                            .getBytes(StandardCharsets.UTF_8)));

            row.put(cacheKey, valueBuilder.build());

        }
        else {
            for (IgniteColumn property : properties) {
                BinaryObjectBuilder valueBuilder = buildEdgeBuilder(attributes);

                String colName = property.getColName();
                valueBuilder.setField(IgniteConstants.PROPERTY_KEY_COL_NAME, colName);

                String colType = property.getColType();
                valueBuilder.setField(IgniteConstants.PROPERTY_TYPE_COL_NAME, colType);

                String colValue = property.getColValue().toString();
                valueBuilder.setField(IgniteConstants.PROPERTY_VALUE_COL_NAME, colValue);
                /*
                 * Build cache entry key
                 */
                List<String> keys = new ArrayList<>(buildAttributeParts(attributes));

                List<String> propertyParts = new ArrayList<>();
                propertyParts.add(IgniteConstants.PROPERTY_KEY_COL_NAME + "::" + colName);
                propertyParts.add(IgniteConstants.PROPERTY_VALUE_COL_NAME + "::" + colValue);

                keys.addAll(propertyParts);
                String cacheKey = Arrays.toString(MessageDigest.getInstance("MD5")
                        .digest(String.join("#", keys)
                                .getBytes(StandardCharsets.UTF_8)));

                row.put(cacheKey, valueBuilder.build());

            }
        }

        return row;
    }

    private Map<String,BinaryObject> buildVertexRow(IgnitePut put) throws Exception {

        Map<String,BinaryObject> row = new HashMap<>();

        List<IgniteColumn> columns = put.getColumns();
        List<IgniteColumn> attributes = columns.stream()
                .filter(column -> !column.getColName()
                        .equals(IgniteConstants.PROPERTY_KEY_COL_NAME))
                .collect(Collectors.toList());

        /*
         * Check how many properties must be set to the cache;
         * Each property is described as an individual entry.
         */
        List<IgniteColumn> properties = columns.stream()
                .filter(column -> column.getColName()
                        .equals(IgniteConstants.PROPERTY_KEY_COL_NAME))
                .collect(Collectors.toList());

        if (properties.isEmpty()) {
            /*
             * There are not properties assigned to the vertex;
             * in this case, a single cache entry is created with
             * a dummy (or empty) property.
             */
            BinaryObjectBuilder valueBuilder = buildVertexBuilder(attributes);

            /* An empty property is added */
            String emptyValue = "*";

            valueBuilder.setField(IgniteConstants.PROPERTY_KEY_COL_NAME, emptyValue);
            valueBuilder.setField(IgniteConstants.PROPERTY_TYPE_COL_NAME, emptyValue);

            valueBuilder.setField(IgniteConstants.PROPERTY_VALUE_COL_NAME, emptyValue);

            ByteBuffer emptyBuffer = ByteBuffer.wrap(emptyValue.getBytes());
            valueBuilder.setField(IgniteConstants.BYTE_BUFFER_COL_NAME, emptyBuffer);
            /*
             * Build cache entry key
             */
            List<String> keys = new ArrayList<>(buildAttributeParts(attributes));

            List<String> propertyParts = new ArrayList<>();
            propertyParts.add(IgniteConstants.PROPERTY_KEY_COL_NAME + "::" + emptyValue);
            propertyParts.add(IgniteConstants.PROPERTY_VALUE_COL_NAME + "::" + emptyValue);

            keys.addAll(propertyParts);
            String cacheKey = Arrays.toString(MessageDigest.getInstance("MD5")
                    .digest(String.join("#", keys)
                            .getBytes(StandardCharsets.UTF_8)));

            row.put(cacheKey, valueBuilder.build());

        }
        else {
            for (IgniteColumn property : properties) {
                BinaryObjectBuilder valueBuilder = buildVertexBuilder(attributes);

                String colName = property.getColName();
                valueBuilder.setField(IgniteConstants.PROPERTY_KEY_COL_NAME, colName);

                String colType = property.getColType();
                valueBuilder.setField(IgniteConstants.PROPERTY_TYPE_COL_NAME, colType);

                String colValue = property.getColValue().toString();
                valueBuilder.setField(IgniteConstants.PROPERTY_VALUE_COL_NAME, colValue);
                /*
                 * Build cache entry key
                 */
                List<String> keys = new ArrayList<>(buildAttributeParts(attributes));

                List<String> propertyParts = new ArrayList<>();
                propertyParts.add(IgniteConstants.PROPERTY_KEY_COL_NAME + "::" + colName);
                propertyParts.add(IgniteConstants.PROPERTY_VALUE_COL_NAME + "::" + colValue);

                keys.addAll(propertyParts);
                String cacheKey = Arrays.toString(MessageDigest.getInstance("MD5")
                        .digest(String.join("#", keys)
                                .getBytes(StandardCharsets.UTF_8)));

                row.put(cacheKey, valueBuilder.build());

           }
        }

        return row;
    }

    private BinaryObjectBuilder buildEdgeBuilder(List<IgniteColumn> attributes) throws Exception {
        BinaryObjectBuilder valueBuilder = connect.getIgnite().binary().builder(name);
        for (IgniteColumn attribute : attributes) {

            switch (attribute.getColName()) {

                case IgniteConstants.ID_COL_NAME: {
                    String colType = attribute.getColType();
                    valueBuilder.setField(IgniteConstants.ID_TYPE_COL_NAME, colType);

                    String colValue = attribute.getColValue().toString();
                    valueBuilder.setField(IgniteConstants.ID_COL_NAME, colValue);

                    break;
                }
                case IgniteConstants.TO_COL_NAME: {
                    String colType = attribute.getColType();
                    valueBuilder.setField(IgniteConstants.TO_TYPE_COL_NAME, colType);

                    String colValue = attribute.getColValue().toString();
                    valueBuilder.setField(IgniteConstants.TO_COL_NAME, colValue);

                    break;
                }
                case IgniteConstants.FROM_COL_NAME: {
                    String colType = attribute.getColType();
                    valueBuilder.setField(IgniteConstants.FROM_TYPE_COL_NAME, colType);

                    String colValue = attribute.getColValue().toString();
                    valueBuilder.setField(IgniteConstants.FROM_COL_NAME, colValue);

                    break;
                }

                case IgniteConstants.LABEL_COL_NAME: {
                    Object colValue = attribute.getColValue();
                    valueBuilder.setField(IgniteConstants.LABEL_COL_NAME, colValue.toString());

                    break;
                }
                case IgniteConstants.CREATED_AT_COL_NAME: {
                    Long colValue = (Long) attribute.getColValue();
                    valueBuilder.setField(IgniteConstants.CREATED_AT_COL_NAME, colValue);

                    break;
                }
                case IgniteConstants.UPDATED_AT_COL_NAME: {
                    Long colValue = (Long) attribute.getColValue();
                    valueBuilder.setField(IgniteConstants.UPDATED_AT_COL_NAME, colValue);

                    break;
                }

                default:
                    break;

            }
        }

        return valueBuilder;
    }

    private BinaryObjectBuilder buildVertexBuilder(List<IgniteColumn> attributes) throws Exception {
        BinaryObjectBuilder valueBuilder = connect.getIgnite().binary().builder(name);
        for (IgniteColumn attribute : attributes) {

            switch (attribute.getColName()) {

                case IgniteConstants.ID_COL_NAME: {
                    String colType = attribute.getColType();
                    valueBuilder.setField(IgniteConstants.ID_TYPE_COL_NAME, colType);

                    String colValue = attribute.getColValue().toString();
                    valueBuilder.setField(IgniteConstants.ID_COL_NAME, colValue);

                    break;
                }

                case IgniteConstants.LABEL_COL_NAME: {
                    Object colValue = attribute.getColValue();
                    valueBuilder.setField(IgniteConstants.LABEL_COL_NAME, colValue.toString());

                    break;
                }
                case IgniteConstants.CREATED_AT_COL_NAME: {
                    Long colValue = (Long) attribute.getColValue();
                    valueBuilder.setField(IgniteConstants.CREATED_AT_COL_NAME, colValue);

                    break;
                }
                case IgniteConstants.UPDATED_AT_COL_NAME: {
                    Long colValue = (Long) attribute.getColValue();
                    valueBuilder.setField(IgniteConstants.UPDATED_AT_COL_NAME, colValue);

                    break;
                }

                default:
                    break;

            }
        }

        return valueBuilder;
    }

    /**
     * This method build the part of the cache key
     * that refers to the (common) attributes.
     */
    private List<String> buildAttributeParts(List<IgniteColumn> attributes) {
        List<String> parts = new ArrayList<>();
        for (IgniteColumn attribute : attributes) {
            String part = attribute.getColName() + "::" + attribute.getColValue().toString();
            parts.add(part);
        }
        return parts;
    }

    /**
     * Retrieve all elements (edges or vertices) that refer
     * to the provided list of identifiers
     */
    public IgniteResult[] get(List<Object> ids) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, connect, ids);

        List<IgniteResult> result = igniteQuery.getResult();
        return result.toArray(new IgniteResult[0]);
    }
    /**
     * Retrieve the element (edge or vertex) that refers
     * to the provided identifier
     */
    public IgniteResult get(Object id) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, connect, id);

        List<IgniteResult> result = igniteQuery.getResult();
        if (result.isEmpty()) return null;
        return result.get(0);
    }

    /**
     * Returns an [IgniteQuery] to retrieve all elements
     */
    public IgniteQuery getAllQuery() {
        return new IgniteAllQuery(name, connect);
    }

    /**
     * Returns an [IgniteQuery] to retrieve all elements
     * that are referenced by a certain label
     */
    public IgniteQuery getLabelQuery(String label) {
        return new IgniteLabelQuery(name, connect, label);
    }
    /**
     * Returns an [IgniteQuery] to retrieve a specified
     * number of (ordered) elements from the beginning
     * of the cache
     */
    public IgniteQuery getLimitQuery(int limit) {
        return new IgniteLimitQuery(name, connect, limit);
    }

    public IgniteQuery getLimitQuery(Object fromId, int limit) {
        return new IgniteLimitQuery(name, connect, fromId, limit);
    }

    public IgniteQuery getLimitQuery(String label, String key, Object inclusiveFrom, int limit, boolean reversed) {
        return new IgniteLimitQuery(name, connect, label, key, inclusiveFrom, limit, reversed);    }
    /**
     * Returns an [IgniteQuery] to retrieve all elements
     * that are referenced by a certain label and share
     * a certain property key and value
     */
    public IgniteQuery getPropertyQuery(String label, String key, Object value) {
        return new IgnitePropertyQuery(name, connect, label, key, value);
    }
    /**
     * Returns an [IgniteQuery] to retrieve all elements
     * that are referenced by a certain label and share
     * a certain property key and value range
     */
    public IgniteQuery getRangeQuery(String label, String key, Object inclusiveFrom, Object exclusiveTo) {
        return new IgniteRangeQuery(name, connect, label, key, inclusiveFrom, exclusiveTo);
    }

    /** EDGE READ SUPPORT **/

    /**
     * Method to find all edges that refer to the provided
     * vertex that match direction and the provided labels
     */
    public IgniteQuery getEdgesQuery(IgniteVertex vertex, Direction direction, String... labels) {
        return new IgniteEdgesQuery(name, connect, vertex.id(), direction, labels);
    }
    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, and a property with
     * a specific value
     */
    public IgniteQuery getEdgesQuery(IgniteVertex vertex, Direction direction, String label,
                                     String key, Object value) {
        return new IgniteEdgesQuery(name, connect, vertex.id(), direction, label, key, value);
    }
    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, property and a range
     * of property values
     */
    public IgniteQuery getEdgesInRangeQuery(IgniteVertex vertex, Direction direction, String label,
                                       String key, Object inclusiveFromValue, Object exclusiveToValue) {
        return new IgniteEdgesInRangeQuery(name, connect, vertex.id(), direction, label,
                key, inclusiveFromValue, exclusiveToValue);
    }

    public IgniteQuery getEdgesWithLimitQuery(IgniteVertex vertex, Direction direction, String label,
                                      String key, Object fromValue, int limit, boolean reversed) {
         return new IgniteEdgesWithLimitQuery(name, connect, vertex.id(), direction, label,
                key, fromValue, limit, reversed);

    }
}
