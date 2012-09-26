/*
   Copyright 2011, Lightbox Technologies, Inc

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.lightboxtechnologies.spectrum;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

import org.codehaus.jackson.map.ObjectMapper;

public class FsEntryHBaseCommon {

  public static final byte LONG = 0;
  public static final byte STRING = 1;
  public static final byte DATE = 2;
  public static final byte JSON = 3;
  public static final byte BYTE_ARRAY = 4;
  public static final byte BUFFER_STREAM = 5;
  public static final byte FILE_STREAM = 6;

  public static byte typeVal(Object o) {
    if (o instanceof Long || o instanceof Integer) {
      return LONG;
    }
    else if (o instanceof String) {
      return STRING;
    }
    else if (o instanceof Date) {
      return DATE;
    }
    else if (o instanceof Map || o instanceof List) {
      return JSON;
    }
    else if (o instanceof byte[]) {
      return BYTE_ARRAY;
    }
    else if (o instanceof BufferProxy) {
      return BUFFER_STREAM;
    }
    else if (o instanceof FileProxy) {
      return FILE_STREAM;
    }
    else {
      return -1;
    }
  }

  public static byte[] createColSpec(Object o, String colName) {
    return createColSpec(typeVal(o), colName);
  }

  public static byte[] createColSpec(byte type, String colName) {
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    stream.write(type);
    try {
      stream.write(Bytes.toBytes(colName));
      return stream.toByteArray();
    }
    catch(IOException e) {
      return null;
    }
  }

  public static String colName(byte[] colSpec) {
    return Bytes.toString(colSpec, 1, colSpec.length-1);
  }

  private static final ObjectMapper mapper = new ObjectMapper();

  @SuppressWarnings("fallthrough")
  public static Object unmarshall(byte[] colSpec, byte[] colVal) {
    byte type = colSpec[0];
    switch (type) {
      case LONG:
        return Bytes.toLong(colVal);
      case STRING:
        return Bytes.toString(colVal);
      case DATE:
        return new Date(Bytes.toLong(colVal));
      case JSON:
        try {
          return mapper.readValue(colVal, 0, colVal.length, Object.class);
        }
        catch (IOException e) {
          // does not parse, failover to byte array
        }
      case BYTE_ARRAY:
        return colVal;
      case BUFFER_STREAM:
        return new BufferProxy(colVal);
      case FILE_STREAM:
        return new FileProxy(Bytes.toString(colVal));
    }
    return null;
  }

  public static void populate(Map<byte[], byte[]> input, Map<String,Object> out, Map<String, StreamProxy> streams) {
    out.clear();
    streams.clear();
    Set<Map.Entry<byte[], byte[]>> set = input.entrySet();
    byte[] key = null;
    for (Map.Entry<byte[], byte[]> pair: set) {
      key = pair.getKey();
      switch (key[0]) {
        case LONG:
        case STRING:
        case DATE:
        case JSON:
        case BYTE_ARRAY:
          out.put(colName(key), unmarshall(key, pair.getValue()));
          break;
        case BUFFER_STREAM:
        case FILE_STREAM:
          streams.put(colName(key), (StreamProxy)unmarshall(key, pair.getValue()));
          break;
      }
    }
  }
}
