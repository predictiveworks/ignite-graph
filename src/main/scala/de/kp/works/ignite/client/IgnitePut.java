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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class IgnitePut extends IgniteMutation {

    private Object id;
    private List<IgniteColumn> columns = new ArrayList<>();

    public IgnitePut(Object id) {
        mutationType = IgniteMutationType.PUT;
        this.id = id;
    }

    public void addColumn(String colName, String colType, String colValue) {
        columns.add(new IgniteColumn(colName, colType, colValue));
    }

    public List<IgniteColumn> getColumns() {
        return columns;
    }

    public List<String> getColumnNames() {
        return columns
                .stream().map(column -> column.getColName())
                .collect(Collectors.toList());
    }

    public Object getId() {
        return id;
    }

}
