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
import de.kp.works.ignite.client.IgniteResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IgniteLimitQuery extends IgniteQuery {
    /**
     * Retrieves a specified number of (ordered) elements
     * from the beginning of the cache. Note, this query
     * is restricted to elements with a numeric identifier.
     */
    public IgniteLimitQuery(String name, IgniteContext context, int limit) {
        super(name, context);
    }

    public IgniteLimitQuery(String name, IgniteContext context, Object fromId, int limit) {
        super(name, context);
    }

    public IgniteLimitQuery(String name, IgniteContext context,
                            String label, String key, Object inclusiveFrom, int limit, boolean reversed) {
        super(name, context);
    }

    @Override
    public List<IgniteResult> getResult() {

        List<IgniteResult> result = new ArrayList<>();
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            return result;

        List<List<?>> sqlResult = getSqlResult();

        // TODO
        return result;

    }

    @Override
    protected void createSql(String cacheName, Map<String, String> fields) {

    }
}
