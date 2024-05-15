/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.memory;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class MultiStringPropertyState extends MultiPropertyState<String> {

    //store compress concatenated values
    private byte[] compressedValues;
    private static Compression compression = Compression.GZIP;

    public MultiStringPropertyState(String name, Iterable<String> values) {
        super(name, Collections.EMPTY_LIST);

        String result = String.join(", ", values);

        int size = result.getBytes().length;
        if (size > 0) {//todo: introduce a threshold
            compressedValues = compress(result.getBytes());
        } else {
            //this.values = (List<String>) values;
        }
    }

    @Override
    public List<String> getValues() {
        return decompress(compressedValues);
    }

    /**
     * Create a multi valued {@code PropertyState} from a list of strings.
     *
     * @param name   The name of the property state
     * @param values The values of the property state
     * @return The new property state of type {@link Type#STRINGS}
     */
    public static PropertyState stringProperty(String name, Iterable<String> values) {

        return new MultiStringPropertyState(name, values);
    }

    private byte[] compress(byte[] value) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            OutputStream compressionOutputStream = compression.getOutputStream(out);
            compressionOutputStream.write(value);
            compressionOutputStream.close();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress data", e);
        }
    }

    @Override
    public Converter getConverter(String value) {
        return Conversions.convert(String.valueOf(decompress(value.getBytes())));
    }

    @Override
    public Type<?> getType() {
        return Type.STRINGS;
    }

    private List<String> decompress(byte[] value) {
        try {
            return Collections.singletonList(new String(compression.getInputStream(new ByteArrayInputStream(value)).readAllBytes()));
        } catch (IOException e) {
            throw new RuntimeException("Failed to decompress data", e);
        }
    }

}
