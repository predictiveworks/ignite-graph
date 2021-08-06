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
import de.kp.works.ignitegraph.IgniteConstants;
import org.apache.ignite.*;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.LinkedHashMap;
import java.util.List;

public class IgniteContext {
    /*
     * Reference to Apache Ignite that is transferred to the key value
     * store to enable cache operations
     */
    private final Ignite ignite;
    private final IgniteConfiguration config;

    public static String namespace;
    private static IgniteContext instance;

    private IgniteContext(IgniteConfiguration config, String namespace){

        this.config = config;
        IgniteContext.namespace = namespace;

        if (this.config == null) ignite = Ignition.start();
        else {
            ignite = Ignition.getOrStart(config);
        }
    }

    public static IgniteContext getInstance(String namespace) {
        return getInstance(null, namespace);
    }

    public static IgniteContext getInstance(IgniteConfiguration config, String namespace) {
        if (instance == null) instance = new IgniteContext(config, namespace);
        return instance;
    }

    public Ignite getIgnite() {
        return ignite;
    }

    public IgniteConfiguration getConfig() {
        return config;
    }

    public IgniteCache<String, BinaryObject> getOrCreateCache(String name) throws Exception {
        /*
         * .withKeepBinary() must not be used here
         */
        if (ignite == null) return null;
        boolean exists = ignite.cacheNames().contains(name);
        if (exists)
            return ignite.cache(name);

        else
            return createCache(name);

    }

    public boolean cacheExists(String cacheName) throws Exception {
        if (ignite == null) throw new Exception("Connecting Ignite failed.");
        return ignite.cacheNames().contains(cacheName);
    }

    public void deleteCache(String cacheName) throws Exception {
        if (ignite == null) throw new Exception("Connecting Ignite failed.");

        boolean exists = ignite.cacheNames().contains(cacheName);
        if (exists)
            ignite.cache(cacheName).destroy();
    }

    private IgniteCache<String,BinaryObject> createCache(String cacheName) throws Exception {
        return createCache(cacheName, CacheMode.REPLICATED);
    }

    private IgniteCache<String,BinaryObject> createCache(String cacheName, CacheMode cacheMode) throws Exception {

        CacheConfiguration<String,BinaryObject> cfg = createCacheCfg(cacheName,cacheMode);
        return ignite.createCache(cfg);

    }

    private CacheConfiguration<String,BinaryObject> createCacheCfg(String table, CacheMode cacheMode) throws Exception {
        /*
         * Defining query entities is the Apache Ignite
         * mechanism to dynamically define a queryable
         * 'class'
         */
        QueryEntity qe = buildQueryEntity(table);

        List<QueryEntity> qes = new java.util.ArrayList<>();
        qes.add(qe);
        /*
         * Specify Apache Ignite cache configuration; it is
         * important to leverage 'BinaryObject' as well as
         * 'setStoreKeepBinary'
         */
        CacheConfiguration<String,BinaryObject> cfg = new CacheConfiguration<>();
        cfg.setName(table);

        cfg.setStoreKeepBinary(false);
        cfg.setIndexedTypes(String.class,BinaryObject.class);

        cfg.setCacheMode(cacheMode);

        cfg.setQueryEntities(qes);
        return cfg;

    }

    /**
     * This method supports the creation of different
     * query entities (or cache schemas)
     */
    private QueryEntity buildQueryEntity(String table) throws Exception {

        QueryEntity qe = new QueryEntity();
        /*
         * The key type of the Apache Ignite cache is set to
         * [String], i.e. an independent identity management is
         * used here
         */
        qe.setKeyType("java.lang.String");
        /*
         * The 'table' is used as table name in select statement
         * as well as the name of 'ValueType'
         */
        qe.setValueType(table);
        /*
         * Define fields for the Apache Ignite cache that is
         * used as one of the data backends.
         */
        if (table.equals(namespace + "_" + IgniteConstants.EDGES)) {
            qe.setFields(buildEdgeFields());
        }
        else if (table.equals(namespace + "_" + IgniteConstants.VERTICES)) {
            qe.setFields(buildVertexFields());
        }
        else
            throw new Exception("Table '" + table + "' is not supported.");

        return qe;
    }

    private LinkedHashMap<String, String> buildEdgeFields() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        /*
         * The edge identifier used by TinkerPop to
         * identify an equivalent of a data row
         */
        fields.put(IgniteConstants.ID_COL_NAME, "java.lang.String");
        /*
         * The edge identifier type to reconstruct the
         * respective value. IgniteGraph supports [Long]
         * as well as [String] as identifier.
         */
        fields.put(IgniteConstants.ID_TYPE_COL_NAME, "java.lang.String");
        /*
         * The edge label used by TinkerPop and IgniteGraph
         */
        fields.put(IgniteConstants.LABEL_COL_NAME, "java.lang.String");
        /*
         * The `TO` vertex description
         */
        fields.put(IgniteConstants.TO_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.TO_TYPE_COL_NAME, "java.lang.String");
        /*
         * The `FROM` vertex description
         */
        fields.put(IgniteConstants.FROM_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.FROM_TYPE_COL_NAME, "java.lang.String");
        /*
         * The timestamp this cache entry has been created.
         */
        fields.put(IgniteConstants.CREATED_AT_COL_NAME, "java.lang.Long");
        /*
         * The timestamp this cache entry has been updated.
         */
        fields.put(IgniteConstants.UPDATED_AT_COL_NAME, "java.lang.Long");
        /*
         * The property section of this cache entry
         */
        fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.PROPERTY_TYPE_COL_NAME, "java.lang.String");
        /*
         * The serialized property value
         */
        fields.put(IgniteConstants.PROPERTY_VALUE_COL_NAME, "java.lang.String");
        /*
         * The [ByteBuffer] representation for
         * the property value
         */
        fields.put(IgniteConstants.BYTE_BUFFER_COL_NAME,"java.nio.ByteBuffer");
        return fields;
    }

    private LinkedHashMap<String, String> buildVertexFields() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        /*
         * The vertex identifier used by TinkerPop to identify
         * an equivalent of a data row
         */
        fields.put(IgniteConstants.ID_COL_NAME, "java.lang.String");
        /*
         * The vertex identifier type to reconstruct the
         * respective value. IgniteGraph supports [Long]
         * as well as [String] as identifier.
         */
        fields.put(IgniteConstants.ID_TYPE_COL_NAME, "java.lang.String");
        /*
         * The vertex label used by TinkerPop and IgniteGraph
         */
        fields.put(IgniteConstants.LABEL_COL_NAME, "java.lang.String");
        /*
         * The timestamp this cache entry has been created.
         */
        fields.put(IgniteConstants.CREATED_AT_COL_NAME, "java.lang.Long");
        /*
         * The timestamp this cache entry has been updated.
         */
        fields.put(IgniteConstants.UPDATED_AT_COL_NAME, "java.lang.Long");
        /*
         * The property section of this cache entry
         */
        fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, "java.lang.String");
        fields.put(IgniteConstants.PROPERTY_TYPE_COL_NAME, "java.lang.String");
        /*
         * The serialized property value
         */
        fields.put(IgniteConstants.PROPERTY_VALUE_COL_NAME, "java.lang.String");
        /*
         * The [ByteBuffer] representation for
         * the property value
         */
        fields.put(IgniteConstants.BYTE_BUFFER_COL_NAME,"java.nio.ByteBuffer");
        return fields;
    }

}
