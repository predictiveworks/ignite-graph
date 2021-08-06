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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class IgniteEdgeSerializer extends IgniteElementSerializer<IgniteEdge> {

    public void write(Kryo kryo, Output output, IgniteEdge edge) {
        super.write(kryo, output, edge);
        byte[] outVBytes = ValueUtils.serialize(edge.outVertex().id());
        output.writeInt(outVBytes.length);
        output.writeBytes(outVBytes);
        byte[] inVBytes = ValueUtils.serialize(edge.inVertex().id());
        output.writeInt(inVBytes.length);
        output.writeBytes(inVBytes);
    }

    public IgniteEdge read(Kryo kryo, Input input, Class<? extends IgniteEdge> type) {

        IgniteEdge edge = super.read(kryo, input, type);
        int outVBytesLen = input.readInt();
        Object outVId = ValueUtils.deserialize(input.readBytes(outVBytesLen));
        int inVBytesLen = input.readInt();
        Object inVId = ValueUtils.deserialize(input.readBytes(inVBytesLen));
        edge.setOutVertex(new IgniteVertex(null, outVId));
        edge.setInVertex(new IgniteVertex(null, inVId));
        return edge;

    }
}
