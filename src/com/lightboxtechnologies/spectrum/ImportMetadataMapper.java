package com.lightboxtechnologies.spectrum;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.sleuthkit.hadoop.core.SKMapper;

public class ImportMetadataMapper
                   extends SKMapper<Object, Text, LongWritable, JsonWritable> {

  private final Log LOG =
    LogFactory.getLog(ImportMetadataMapper.class.getName());

  private final FsEntry Entry = new FsEntry();
  private final byte[] Id = new byte[FsEntryUtils.ID_LENGTH];
  private final MessageDigest Hasher = FsEntryUtils.getHashInstance("MD5");

  private HTable Table;

  protected final List<Map<String,Object>> Extents =
    new ArrayList<Map<String,Object>>();
  protected final Map<String,Object> Output = new HashMap<String,Object>();

  protected final LongWritable Offset = new LongWritable();
  protected final JsonWritable JsonOutput = new JsonWritable();

  @Override
  public void setup(Context context) throws IOException {
    super.setup(context);

    final Configuration conf = context.getConfiguration();
    Table = new HTable(conf, HBaseTables.ENTRIES_TBL_B);

    // ensure that the extents list is put into the output map
    Output.put("extents", Extents);
  }

  @Override
  protected void map(Object key, Text value, Context context)
                                     throws IOException, InterruptedException {
    // read the JSON produced by fsrip
    if (!Entry.parseJson(value.toString())) {
      LOG.warn("JSON parse failed: " + value.toString());
      return;
    }

    // write the metadata to HBase
    FsEntryUtils.makeFsEntryKey(
      Id,
      getImageID(),
      ((String) Entry.get("path")).getBytes(),
      (Integer) Entry.get("dirIndex"),
      Hasher
    );

    final Put p = FsEntryPut.create(Id, Entry, HBaseTables.ENTRIES_COLFAM_B);
    if (p.isEmpty()) {
      // TODO: inidicate failure here?
      return;
    }
    Table.put(p);

    // extract the extents data
    // TODO: This try/catch is nasty, fix it so we check return values
    // instead of throwing on bad input.
    try {
      final List<Map> attrs = (List<Map>) Entry.get("attrs");
      if (attrs == null) {
        LOG.info(Entry.fullPath() + " has no attributes");
        return;
      }

      long fsByteOffset = ((Number) Entry.get("fs_byte_offset")).longValue();
      long fsBlockSize = ((Number) Entry.get("fs_block_size")).longValue();

      for (final Map attribute : attrs) {
        final long flags = ((Number) attribute.get("flags")).longValue();
        final long type = ((Number) attribute.get("type")).longValue();
        if ((flags & 0x03) > 0 && (type & 0x81) > 0) {
          // flags & type indicate this attribute has nonresident data
          final Object nrds = attribute.get("nrd_runs");
          if (nrds == null) {
            LOG.warn(Entry.fullPath() + " had an nrd attr with null runs");
            return;
          }
          final List<Map<String, Object>> runs = (List<Map<String,Object>>)nrds;
          if (setExtents(runs, fsByteOffset, fsBlockSize) && !Extents.isEmpty()) {
            Offset.set(((Number)Extents.get(0).get("addr")).longValue());
            Output.put("size", Entry.get("size"));
            Output.put("fp", Entry.fullPath());
            Output.put("id", Hex.encodeHexString(Id));
            JsonOutput.set(Output);
            LOG.info(JsonOutput.toString());
            context.write(Offset, JsonOutput);
            break; // take the first one we get... FIXME someday
          }
        }
      }
    }
    catch (ClassCastException e) {
      LOG.warn(errorString(e));
      e.printStackTrace();
    }
    catch (NullPointerException e) {
      LOG.warn(errorString(e));
      e.printStackTrace();
    }
    catch (Exception e) {
      LOG.warn(errorString(e));
      e.printStackTrace();
    }
  }

  protected boolean setExtents(List<Map<String,Object>> dataRuns, long fsByteOffset, long fsBlockSize) {
    Extents.clear();
    if (dataRuns.isEmpty()) {
      return false;
    }

    for (Map<String,Object> run: dataRuns) {
      Map<String,Object> dr = new HashMap<String,Object>();
      dr.put("flags", run.get("flags"));
      dr.put("addr", (((Number)run.get("addr")).longValue() * fsBlockSize) + fsByteOffset);
      dr.put("offset", ((Number)run.get("offset")).longValue() * fsBlockSize);
      dr.put("len", ((Number)run.get("len")).longValue() * fsBlockSize);
      Extents.add(dr);
    }
    return true;
  }

  protected String errorString(Exception e) {
    final StringBuilder b = new StringBuilder("Exception on ");
    b.append(Hex.encodeHexString(Id));
    b.append(":");
    b.append(Entry.fullPath());
    b.append(": ");
    try {
      b.append(Entry.toString());
    }
    catch (NullPointerException npe) {
      // FIXME: bugger, this happens becuase FsEntry violates the Map contract
      b.append("null");
    }
    return b.toString();
  }
}
