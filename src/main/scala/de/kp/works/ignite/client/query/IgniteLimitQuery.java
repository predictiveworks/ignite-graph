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
import de.kp.works.ignitegraph.IgniteConstants;

import java.util.HashMap;
import java.util.Map;

public class IgniteLimitQuery extends IgniteQuery {
    /**
     * Retrieves a specified number of (ordered) elements
     * from the beginning of the cache. Note, this query
     * is restricted to elements with a numeric identifier.
     */
    public IgniteLimitQuery(String cacheName, IgniteContext context, int limit) {
        super(cacheName, context);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();

        fields.put(IgniteConstants.LIMIT_VALUE, String.valueOf(limit));
        createSql(fields);
    }

    public IgniteLimitQuery(String cacheName, IgniteContext context, Object fromId, int limit) {
        super(cacheName, context);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();

        fields.put(IgniteConstants.FROM_ID_VALUE, fromId.toString());
        fields.put(IgniteConstants.LIMIT_VALUE, String.valueOf(limit));

        createSql(fields);
    }

    public IgniteLimitQuery(String cacheName, IgniteContext context,
                            String label, String key, Object inclusiveFrom, int limit, boolean reversed) {
        super(cacheName, context);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();

        fields.put(IgniteConstants.LABEL_COL_NAME, label);
        fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, key);

        fields.put(IgniteConstants.INCLUSIVE_FROM_VALUE, inclusiveFrom.toString());
        fields.put(IgniteConstants.LIMIT_VALUE, String.valueOf(limit));

        fields.put(IgniteConstants.REVERSED_VALUE, String.valueOf(reversed));
        createSql(fields);
    }

    @Override
    protected void createSql(Map<String, String> fields) {
        try {
            buildSelectPart();
            /*
             * Build the `clause` of the SQL statement
             * from the provided fields
             */
            sqlStatement += " where " + IgniteConstants.LABEL_COL_NAME;
            sqlStatement += " = '" + fields.get(IgniteConstants.LABEL_COL_NAME) + "'";

            sqlStatement += " and " + IgniteConstants.PROPERTY_KEY_COL_NAME;
            sqlStatement += " = '" + fields.get(IgniteConstants.PROPERTY_KEY_COL_NAME) + "'";

            // TODO

        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
