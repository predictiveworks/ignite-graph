package de.kp.works.ignitegraph;
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

import java.util.Map;

public class LabelMetadata {

    private final Key key;
    private final ValueType idType;
    private final Map<String, ValueType> propertyTypes;
    protected final Long createdAt;
    protected Long updatedAt;

    public LabelMetadata(ElementType type, String label, ValueType idType,
                         Long createdAt, Long updatedAt, Map<String, ValueType> propertyTypes) {
        this.key = new Key(type, label);
        this.idType = idType;
        this.propertyTypes = propertyTypes;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public Key key() {
        return key;
    }

    public ElementType type() {
        return key.type();
    }

    public String label() {
        return key.label();
    }

    public ValueType idType() {
        return idType;
    }

    public Map<String, ValueType> propertyTypes() {
        return propertyTypes;
    }

    public Long createdAt() {
        return createdAt;
    }

    public Long updatedAt() {
        return updatedAt;
    }

    public void updatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return key.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LabelMetadata that = (LabelMetadata) o;

        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    public static class Key {
        private final ElementType type;
        private final String label;

        public Key(ElementType type, String label) {
            this.type = type;
            this.label = label;
        }

        public ElementType type() {
            return type;
        }

        public String label() {
            return label;
        }

        @Override
        public String toString() {
            return type + " LABEL " + " " + label;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;

            if (type != key.type) return false;
            return (label.equals(key.label));
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + label.hashCode();
            return result;
        }
    }
}
