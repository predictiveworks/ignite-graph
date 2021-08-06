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
import de.kp.works.ignitegraph.IgniteConstants;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IgniteEdgesQuery extends IgniteQuery {

    public IgniteEdgesQuery(String cacheName, IgniteContext context,
                            Object vertex, Direction direction, String... labels) {
        super(cacheName, context);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();
        vertexToFields(vertex, direction, fields);

        fields.put(IgniteConstants.LABEL_COL_NAME, String.join(",", labels));
        createSql(cacheName, fields);

    }

    public IgniteEdgesQuery(String cacheName, IgniteContext context,
                            Object vertex, Direction direction, String label, String key, Object value) {
        super(cacheName, context);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();
        vertexToFields(vertex, direction, fields);

        fields.put(IgniteConstants.LABEL_COL_NAME, label);

        fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, key);
        fields.put(IgniteConstants.PROPERTY_VALUE_COL_NAME, value.toString());

        createSql(cacheName, fields);

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
