package com.lightboxtechnologies.spectrum;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class FsEntryPut {
  protected FsEntryPut() {}

  private static final ObjectMapper mapper = new ObjectMapper();

  public static void add(Put p, Map<String,?> map, byte[] colFam) {
    // should this be in the -Common class?
    byte[] col = null,
           binVal = null;
    String key = null;
    Object val = null;

    for (Map.Entry<String,?> pair : map.entrySet()) {
      key = pair.getKey();
      val = pair.getValue();
      if (val == null) {
        throw new RuntimeException("val was null for key " + key);
      }

      col = FsEntryHBaseCommon.createColSpec(val, key);
      switch (col[0]) {
        case FsEntryHBaseCommon.STRING:
          binVal = Bytes.toBytes((String)val);
          break;
        case FsEntryHBaseCommon.LONG:
          binVal = Bytes.toBytes(((Number)val).longValue());
          break;
        case FsEntryHBaseCommon.DATE:
          binVal = Bytes.toBytes(((Date)val).getTime());
          break;
        case FsEntryHBaseCommon.JSON:
          try {
            binVal = mapper.writeValueAsBytes(val);
          }
          catch (IOException e) {
            throw new RuntimeException("Failed to serialize to JSON: " + val);
          }
          break;
        case FsEntryHBaseCommon.BYTE_ARRAY:
          binVal = (byte[])val;
          break;
        case FsEntryHBaseCommon.BUFFER_STREAM:
          binVal = ((BufferProxy)val).getBuffer();
          break;
        case FsEntryHBaseCommon.FILE_STREAM:
          binVal = Bytes.toBytes(((FileProxy)val).getPath());
          break;
        default:
          throw new RuntimeException("Didn't get something that could be converted to a byte[], " + val.toString() + " for key " + key);
      }
      p.add(colFam, col, binVal);
    }
  }

  public static Put create(byte[] key, Map<String,Object> map, byte[] colFam) {
    final Put p = new Put(key);
    add(p, map, colFam);
    return p;
  }

  public static Put create(ImmutableBytesWritable key, Map<String,Object> map, byte[] colFam) {
    return create(key.get(), map, colFam);
  }

  @SuppressWarnings("unchecked")
  public static Put create(byte[] key, FsEntry entry, byte[] colFam) {
    final Put p = new Put(key);

    // FIXME: this logic should really be in FsEntry proper
    final Object o = entry.get("attrs");
    if (o != null && null == entry.getStreams().get("Content")) {
      String data = null;
      for (Map<String,Object> attr : (List<Map<String,Object>>)o) {
        final Number flags = (Number) attr.get("flags"),
                     type  = (Number) attr.get("type");
        final String name = (String) attr.get("name");
        if (flags != null && (flags.longValue() & 0x04) != 0
          && type != null && (type.longValue() & 0x80) != 0
          && name != null && name.equals("$Data"))
        {
          data = (String) attr.get("rd_buf");
          break;
        }
      }

      if (data != null && data.length() > 0) {
        byte[] v;
        try {
          v = Hex.decodeHex(data.toCharArray());
        }
        catch (DecoderException e) {
          throw new IllegalArgumentException(e);
        }

        p.add(
          colFam,
          FsEntryHBaseCommon.createColSpec(
            FsEntryHBaseCommon.BUFFER_STREAM,
            "Content"
          ),
          v
        );
      }
    }

    add(p, entry.getChangedItems(), colFam);
    // add(p, entry.getStreams(), colFam);

    return p;
  }
}
