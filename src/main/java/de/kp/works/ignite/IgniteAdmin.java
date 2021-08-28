package de.kp.works.ignite;
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

import de.kp.works.ignite.client.IgniteConnect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteAdmin {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteAdmin.class);
    private final IgniteConnect connect;

    private final String NO_CONNECT_INITIALIZATION = "IgniteConnect is not initialized.";

    public IgniteAdmin(IgniteConnect connect) {
        this.connect = connect;
    }

    public boolean tableExists(String name) throws Exception {
        return connect.cacheExists(name);
    }

    public void createTable(String name) {

        try {

            if (this.connect == null)
                throw new Exception(NO_CONNECT_INITIALIZATION);

            this.connect.getOrCreateCache(name);

        } catch (Exception e) {
            LOGGER.error("Cache creation failed.", e);
        }
    }

    public void dropTable(String name) throws Exception {

        if (this.connect == null)
            throw new Exception(NO_CONNECT_INITIALIZATION);

        this.connect.deleteCache(name);

    }
    /**
     * This method creates an [IgniteTable] and provides
     * access to the underlying cache. Note, this method
     * does not verify whether the cache exists or not.
     */
    public IgniteTable getTable(String tableName) {
        if (connect == null) {
            LOGGER.error(NO_CONNECT_INITIALIZATION);
            return null;
        }

        return new IgniteTable(tableName, connect);
    }

}