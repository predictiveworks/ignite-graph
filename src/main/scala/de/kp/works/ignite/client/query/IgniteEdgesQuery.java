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
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        createSql(fields);

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

        createSql(fields);

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
    protected void createSql(Map<String, String> fields) {
        try {
            buildSelectPart();
            /*
             * Build `where` clause and thereby distinguish
             * between a single or multiple label values
             */
            if (fields.containsKey(IgniteConstants.TO_COL_NAME)) {
                sqlStatement += " where " + IgniteConstants.TO_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.TO_COL_NAME) + "'";
            }
            if (fields.containsKey(IgniteConstants.FROM_COL_NAME)) {
                sqlStatement += " where " + IgniteConstants.FROM_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.FROM_COL_NAME) + "'";
            }
            if (fields.containsKey(IgniteConstants.PROPERTY_KEY_COL_NAME)) {
                /*
                 * A query for those edges of a certain vertex that
                 * match a specific label and value of the selected
                 * property.
                 */
                sqlStatement += " and " + IgniteConstants.PROPERTY_KEY_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.PROPERTY_KEY_COL_NAME) + "'";

                sqlStatement += " and " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.PROPERTY_VALUE_COL_NAME) + "'";

                sqlStatement += " and " + IgniteConstants.LABEL_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.LABEL_COL_NAME) + "'";

            }
            else {
                sqlStatement += " and " + IgniteConstants.LABEL_COL_NAME;
                String labels = fields.get(IgniteConstants.LABEL_COL_NAME);

                String[] tokens = labels.split(",");
                if (tokens.length == 1) {
                    sqlStatement += " = '" + tokens[0] + "'";
                } else {
                    List<String> inPart = Stream.of(tokens)
                            .map(token -> "'" + token + "'").collect(Collectors.toList());

                    sqlStatement += " in(" + String.join(",", inPart) + ")";
                }

            }


        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
