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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.IgniteConfiguration;

public class IgniteClient {
    /*
     * The static reference to the Apache Ignite client; [IgniteContext]
     * is designed as a singleton and this call is used to initialize
     * this client
     */
    private static IgniteUtils igniteUtils;

    private static IgniteClient instance;

    private IgniteClient(IgniteConfiguration igniteConfig, String namespace) throws Exception {
        /*
         * Prepare Apache Ignite caches that are leveraged by IgniteGraph;
         * note, this must be done before any graph operations is initiated
         * (even the configuration of a certain graph)
         */
        igniteUtils = IgniteUtils.getInstance(igniteConfig, namespace);

        Ignite ignite = igniteUtils.getIgnite();
        if (ignite == null)
            throw new Exception("[IgniteGraph] A problem occurred while trying to initialize Apache Ignite.");

    }
    /**
     * Retrieve an instance of IgniteClient without
     * any externally provided configuration options
     */
    public static IgniteClient getInstance(String namespace) throws Exception {
        return getInstance(null, namespace);
    }

    public static IgniteClient getInstance(IgniteConfiguration config, String namespace) throws Exception {
        if (instance == null) instance = new IgniteClient(config, namespace);
        return instance;
    }

    public IgniteUtils getContext() {
        return igniteUtils;
    }
    public Ignite getIgnite() {
        return igniteUtils.getIgnite();
    }

    public boolean cacheExists(String cacheName) throws Exception {
        return igniteUtils.cacheExists(cacheName);
    }

    public void createCache(String cacheName) throws Exception {
        IgniteCache<String, BinaryObject> cache = igniteUtils.getOrCreateCache(cacheName);
        if (cache == null)
            throw new Exception("Connection to Ignited failed. Could not create cache.");
        /*
         * Rebalancing is called here in case of partitioned
         * Apache Ignite caches; the default configuration,
         * however, is to use replicated caches
         */
        cache.rebalance().get();
    }

    public void deleteCache(String cacheName) throws Exception {
        igniteUtils.deleteCache(cacheName);
    }
}
