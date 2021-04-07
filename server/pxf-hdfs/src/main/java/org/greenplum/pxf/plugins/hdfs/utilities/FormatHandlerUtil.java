package org.greenplum.pxf.plugins.hdfs.utilities;

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

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.error.PxfRuntimeException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// Re-used from GPHDFS
// https://github.com/greenplum-db/gpdb/blob/3b0bfdc169fab7f686276be7eccb024a5e29543c/gpAux/extensions/gphdfs/src/java/1.2/com/emc/greenplum/gpdb/hadoop/formathandler/util/FormatHandlerUtil.java
@Slf4j
public class FormatHandlerUtil {
    public static Object decodeString(Schema schema, String value, boolean isTopLevel, char delimiter) {
        log.debug("schema={}, value={}, isTopLevel={}", schema, value, isTopLevel);
        Schema.Type fieldType = schema.getType();
        if (fieldType == Schema.Type.ARRAY) {
            if (value == null) {
                return null;
            }

            List<Object> foo = new ArrayList<>();
            String[] splits = getArraySplits(value.toCharArray(), delimiter);
            for (String s : splits) {
                foo.add(decodeString(schema.getElementType(), s, false, delimiter));
            }
            return foo;
        } else {
            log.debug("not an array");
            if (fieldType == Schema.Type.UNION) {
                log.debug("is a union");
                schema = firstNotNullSchema(schema.getTypes());
                if (schema == null) {
                    throw new PxfRuntimeException(String.format("%s is a union but only has null type", schema));
                }

                fieldType = schema.getType();
                if (fieldType == Schema.Type.ARRAY) {
                    log.debug("first non-null type is array");
                    return decodeString(schema, value, isTopLevel, delimiter);
                }
            }
            if (StringUtils.equals(value, "NULL") && !isTopLevel) {
                return null;
            }

            switch (fieldType) {
                case INT:
                    return Integer.parseInt(value);
                case DOUBLE:
                    return Double.parseDouble(value);
                case STRING:
                case RECORD:
                case ENUM:
                case MAP:
                    // FIXME: return records as strings to maintain old behavior
                    return value;
                case FLOAT:
                    return Float.parseFloat(value);
                case LONG:
                    return Long.parseLong(value);
                case BYTES:
                    return octString2byteArray(value);
                case BOOLEAN:
                    return Boolean.parseBoolean(value);
                default:
                    throw new PxfRuntimeException(String.format("type: %s cannot be supported now", fieldType));
            }
        }
    }

    public static String[] getArraySplits(char[] value, char delimiter) {
        List<Integer> posList = new ArrayList<>();
        posList.add(0);

        if (value[0] != '{' || value[value.length - 1] != '}') {
            throw new PxfRuntimeException(String.format("array dim mismatch, rawData: %s", new String(value)));
        }

        int depth = 0;
        boolean inQuoted = false;
        for (int i = 0; i < value.length; i++) {
            if (value[i] == delimiter) {
                if (depth == 1 && !inQuoted) {
                    posList.add(i);
                }

                continue;
            }
            switch (value[i]) {
                case '{':
                    if (!inQuoted) {
                        depth++;
                    }
                    break;
                case '}':
                    if (!inQuoted) {
                        depth--;
                        if (depth == 0) {
                            posList.add(i);
                        }
                    }
                    break;
                case '\"':
                    if (isQuote(value, i)) {
                        inQuoted = !inQuoted;
                    }
                    break;
                default:
                    break;
            }
        }

        String[] subStrings = new String[posList.size() - 1];
        for (int i = 0; i < posList.size() - 1; i++) {
            subStrings[i] = new String(value, posList.get(i) + 1, posList.get(i+1) - posList.get(i) - 1);
        }
        return subStrings;
    }

    private static boolean isQuote(char[] value, int index) {
        int num = 0;
        for (int i = index - 1; i >= 0; i--) {
            if (value[i] == '\\') {
                num++;
            } else {
                break;
            }
        }

        return num % 2 == 0;
    }

    public static ByteBuffer octString2byteArray(String value) {
        List<Byte> byteList = new ArrayList<>();
        int index = 0, length = value.length();
        if (value.charAt(0) == '"') {
            index = 1;
            length -= 1;
        }

        while (index < length) {
            if (value.charAt(index) == '\\' && value.charAt(index + 1) == '\\') {
                index += 2;
                if (value.charAt(index) == '\\' && value.charAt(index + 1) == '\\') {
                    byteList.add((byte) 0x5C);
                    index += 2;
                }else {
                    byteList.add(octString2Byte(value.substring(index, index + 3)));
                    index += 3;
                }
            }else if (value.charAt(index) == '\\') {
                byteList.add((byte) value.charAt(index+1));
                index += 2;
            }else {
                byteList.add((byte) value.charAt(index));
                index++;
            }
        }

        byte[] ba = new byte[byteList.size()];
        for (int i = 0; i < byteList.size(); i++) {
            ba[i] = byteList.get(i);
        }

        return ByteBuffer.wrap(ba);
    }

    private static byte octString2Byte(String string) {
        int num = 0;
        for (int i = 0; i < string.length(); i++) {
            num *= 8;
            num += (string.charAt(i) - '0');
        }
        return (byte) num;
    }

    public static Schema firstNotNullSchema(List<Schema> types) {
        for (Schema schema : types) {
            log.debug("check if {} is not null", schema);
            if (schema.getType() != Schema.Type.NULL) {
                return schema;
            }
        }

        return null;
    }
}
