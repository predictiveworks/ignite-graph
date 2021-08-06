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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public class IgniteElementSerializer<E extends IgniteElement> extends Serializer<E> {

    public void write(Kryo kryo, Output output, E element) {
        byte[] idBytes = ValueUtils.serialize(element.id());
        output.writeInt(idBytes.length);
        output.writeBytes(idBytes);
        output.writeString(element.label());
        output.writeLong(element.createdAt());
        output.writeLong(element.updatedAt());
        Map<String, Object> properties = element.getProperties();
        output.writeInt(properties.size());
        properties.forEach((key, value) -> {
            output.writeString(key);
            byte[] bytes = ValueUtils.serialize(value);
            output.writeInt(bytes.length);
            output.writeBytes(bytes);
        });
    }

    public E read(Kryo kryo, Input input, Class<? extends E> type) {
        int idBytesLen = input.readInt();
        Object id = ValueUtils.deserialize(input.readBytes(idBytesLen));
        String label = input.readString();
        long createdAt = input.readLong();
        long updatedAt = input.readLong();
        int propertiesSize = input.readInt();
        Map<String, Object> properties = new HashMap<>();
        for (int i = 0; i < propertiesSize; i++) {
            String key = input.readString();
            int bytesLen = input.readInt();
            Object value = ValueUtils.deserialize(input.readBytes(bytesLen));
            properties.put(key, value);
        }
        try {
            Constructor<? extends E> ctor = type.getDeclaredConstructor(
                    IgniteGraph.class, Object.class, String.class, Long.class, Long.class, Map.class);
            return ctor.newInstance(null, id, label, createdAt, updatedAt, properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
