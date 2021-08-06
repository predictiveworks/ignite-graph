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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteAdmin {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteAdmin.class);
    private final IgniteClient client;

    private final String NO_CLIENT_INITIALIZATION = "IgniteClient is not initialized.";

    public IgniteAdmin(IgniteClient client) {
        this.client = client;
    }

    public boolean tableExists(String name) throws Exception {
        return client.cacheExists(name);
    }

    public void createTable(String name) {

        try {

            if (this.client == null)
                throw new Exception(NO_CLIENT_INITIALIZATION);

            this.client.createCache(name);

        } catch (Exception e) {
            LOGGER.error("Cache creation failed.", e);
        }
    }

    public void dropTable(String name) throws Exception {

        if (this.client == null)
            throw new Exception(NO_CLIENT_INITIALIZATION);

        this.client.deleteCache(name);

    }
    /**
     * This method creates an [IgniteTable] and provides
     * access to the underlying cache. Note, this method
     * does not verify whether the cache exists or not.
     */
    public IgniteTable getTable(String tableName) {
        if (client == null) {
            LOGGER.error(NO_CLIENT_INITIALIZATION);
            return null;
        }

        IgniteContext context = client.getContext();
        if (context == null) {
            LOGGER.error("IgniteContext is not initialized.");
            return null;
        }

        return new IgniteTable(tableName, context);
    }

}
