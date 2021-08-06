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

import de.kp.works.ignite.client.query.IgniteAllQuery;
import de.kp.works.ignite.client.query.IgniteGetQuery;
import de.kp.works.ignite.client.query.IgniteLabelQuery;
import de.kp.works.ignite.client.query.IgniteQuery;
import de.kp.works.ignitegraph.IgniteConstants;
import de.kp.works.ignitegraph.IgniteVertex;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

public class IgniteTable {

    private final String name;
    private final IgniteContext context;

    public IgniteTable(String name, IgniteContext context) {
        this.name = name;
        this.context = context;
    }

    /**
     * This method adds or updates an Ignite cache entry
     */
    public boolean checkAndPut(IgnitePut put) {

        if (context == null) return false;
        if (context.getIgnite() == null) return false;

        try {

            IgniteCache<String, BinaryObject> cache = context.getOrCreateCache(name);

            Map<String, BinaryObject> row = buildRow(put);
            for (Map.Entry<String,BinaryObject> entry : row.entrySet()) {
                 cache.put(entry.getKey(), entry.getValue());
            }
            return true;

        } catch (Exception e) {
            return false;
        }

    }

    private Map<String,BinaryObject> buildRow(IgnitePut put) throws Exception {

        if (name.equals(IgniteContext.namespace + "_" + IgniteConstants.EDGES)) {
            return buildEdgeRow(put);
        }
        if (name.equals(IgniteContext.namespace + "_" + IgniteConstants.VERTICES)) {
            return buildVertexRow(put);
        }
        throw new Exception("Table '" + name +  "' is not supported.");

    }

    private Map<String, BinaryObject> buildEdgeRow(IgnitePut put) throws NoSuchAlgorithmException {

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

                ByteBuffer colBuffer = ByteBuffer.wrap(property.getColBytes());
                valueBuilder.setField(IgniteConstants.BYTE_BUFFER_COL_NAME, colBuffer);
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

    private Map<String,BinaryObject> buildVertexRow(IgnitePut put) throws NoSuchAlgorithmException {

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

                ByteBuffer colBuffer = ByteBuffer.wrap(property.getColBytes());
                valueBuilder.setField(IgniteConstants.BYTE_BUFFER_COL_NAME, colBuffer);
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

    private BinaryObjectBuilder buildEdgeBuilder(List<IgniteColumn> attributes) {
        BinaryObjectBuilder valueBuilder = context.getIgnite().binary().builder(name);
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

    private BinaryObjectBuilder buildVertexBuilder(List<IgniteColumn> attributes) {
        BinaryObjectBuilder valueBuilder = context.getIgnite().binary().builder(name);
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

    // TODO

    public void batch(List<IgniteMutation> mutations, Object[] results) {
        // TODO
    }

    /**
     * Retrieve all elements (edges or vertices) that refer
     * to the provided list of identifiers
     */
    public IgniteResult[] get(List<Object> ids) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, context, ids);

        List<IgniteResult> result = igniteQuery.getResult();
        return result.toArray(new IgniteResult[0]);
    }
    /**
     * Retrieve the element (edge or vertex) that refers
     * to the provided identifier
     */
    public IgniteResult get(Object id) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, context, id);

        List<IgniteResult> result = igniteQuery.getResult();
        if (result.isEmpty()) return null;
        return result.get(0);
    }

    /**
     * Returns an [IgniteQuery] to retrieve all elements
     */
    public IgniteQuery getAllQuery() {
        return new IgniteAllQuery(name, context);
    }

    /**
     * Returns an [IgniteQuery] to retrieve all elements
     * that are referenced by a certain label
     */
    public IgniteQuery getLabelQuery(String label) {
        return new IgniteLabelQuery(name, context, label);
    }
    /**
     * Returns an [IgniteQuery] to retrieve a specified
     * number of (ordered) elements from the beginning
     * of the cache
     */
    public IgniteQuery getLimitQuery(int limit) {
        // TODO
        return null;
    }

    public IgniteQuery getLimitQuery(Object fromId, int limit) {
        // TODO
        return null;
    }

    public IgniteQuery getLimitQuery(String label, String key, Object inclusiveFrom, int limit, boolean reversed) {
        // TODO
        return null;
    }
    /**
     * Returns an [IgniteQuery] to retrieve all elements
     * that are referenced by a certain label and share
     * a certain property key and value
     */
    public IgniteQuery getPropertyQuery(String label, String key, Object value) {
        // TODO
        return null;
    }
    /**
     * Returns an [IgniteQuery] to retrieve all elements
     * that are referenced by a certain label and share
     * a certain property key and value range
     */
    public IgniteQuery getRangeQuery(String label, String key, Object inclusiveFrom, Object exclusiveTo) {
        // TODO
        return null;
    }

    /** EDGE READ SUPPORT **/

    /**
     * Method to find all edges that refer to the provided
     * vertex that match direction and the provided labels
     */
    public IgniteQuery getEdgesQuery(IgniteVertex vertex, Direction direction, String... labels) {
        // TODO
        return null;
    }
    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, and a property with
     * a specific value
     */
    public IgniteQuery getEdgesQuery(IgniteVertex vertex, Direction direction, String label,
                                     String key, Object value) {
        // TODO
        return null;
    }
    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, property and a range
     * of property values
     */
    public IgniteQuery getEdgesInRangeQuery(IgniteVertex vertex, Direction direction, String label,
                                       String key, Object inclusiveFromValue, Object exclusiveToValue) {
        // TODO
        return null;
    }

    public IgniteQuery getEdgesWithLimitQuery(IgniteVertex vertex, Direction direction, String label,
                                      String key, Object fromValue, int limit, boolean reversed) {
        // TODO
        return null;

    }
}
