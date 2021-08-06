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

public class LabelConnection {

    private final String outVertexLabel;
    private final String edgeLabel;
    private final String inVertexLabel;
    private final Long createdAt;

    public LabelConnection(String outVertexLabel, String edgeLabel, String inVertexLabel, Long createdAt) {
        this.outVertexLabel = outVertexLabel;
        this.edgeLabel = edgeLabel;
        this.inVertexLabel = inVertexLabel;
        this.createdAt = createdAt;
    }

    public String outVertexLabel() {
        return outVertexLabel;
    }

    public String edgeLabel() {
        return edgeLabel;
    }

    public String inVertexLabel() {
        return inVertexLabel;
    }

    public Long createdAt() {
        return createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LabelConnection that = (LabelConnection) o;

        if (!outVertexLabel.equals(that.outVertexLabel)) return false;
        if (!edgeLabel.equals(that.edgeLabel)) return false;
        return inVertexLabel.equals(that.inVertexLabel);
    }

    @Override
    public int hashCode() {
        int result = outVertexLabel.hashCode();
        result = 31 * result + edgeLabel.hashCode();
        result = 31 * result + inVertexLabel.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LABEL CONNECTION [" + outVertexLabel + " - " + edgeLabel + " -> " + inVertexLabel + "]";
    }
}
