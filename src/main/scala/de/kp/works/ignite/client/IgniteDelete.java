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

public class IgniteDelete extends IgniteMutation {

    private Object id;

    private final List<IgniteColumn> columns = new ArrayList<>();

    public IgniteDelete(Object id) {
        this.id = id;
    }

    public void addColumn(String colName) {
        columns.add(new IgniteColumn(colName));
    }

    public void addColumn(String colName, String colType) {
        columns.add(new IgniteColumn(colName, colType));
    }

    /**
     * The colValue is a timestamp that controls deletion.
     */
    public void addColumn(String colName, String colType, Object colValue, byte[] colBytes) {
        columns.add(new IgniteColumn(colName, colType, colValue, colBytes));
    }

}
