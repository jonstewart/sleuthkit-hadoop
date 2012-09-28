package com.lightboxtechnologies.spectrum;

import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.sleuthkit.hadoop.core.SKJobFactory;
import org.sleuthkit.hadoop.core.SKMapper;

class ExtentsExtractorMapper
  extends SKMapper<ImmutableHexWritable, FsEntry, LongWritable, JsonWritable> {

  protected final Log LOG = LogFactory.getLog(ExtentsExtractorMapper.class.getName());
  protected final LongWritable Offset = new LongWritable();
  protected final Map<String,Object> Output = new HashMap<String,Object>();
  protected final List<Map<String,Object>> Extents =
    new ArrayList<Map<String,Object>>();
  protected final JsonWritable JsonOutput = new JsonWritable();

  public ExtentsExtractorMapper() {
    Output.put("extents", Extents);
  }

  protected static String errorString(ImmutableHexWritable key, FsEntry value, Exception e) {
    final StringBuilder b = new StringBuilder("Exception on ");
    b.append(key.toString());
    b.append(":");
    b.append(value.fullPath());
    b.append(": ");
    b.append(e.toString());
    return b.toString();
  }

  protected boolean setExtents(List<Map<String,Object>> dataRuns, long fsByteOffset, long fsBlockSize) {
    Extents.clear();
    if (!dataRuns.isEmpty()) {
      for (Map<String,Object> run: dataRuns) {
        Map<String,Object> dr = new HashMap<String,Object>();
        dr.put("flags", run.get("flags"));
        dr.put("addr", (((Number)run.get("addr")).longValue() * fsBlockSize) + fsByteOffset);
        dr.put("offset", ((Number)run.get("offset")).longValue() * fsBlockSize);
        dr.put("len", ((Number)run.get("len")).longValue() * fsBlockSize);
        Extents.add(dr);
        return true;
      }
    }
    return false;
  }

  @Override
  public void map(ImmutableHexWritable key, FsEntry value, Context context) throws IOException, InterruptedException {
    try {
      final List<Map> attrs = (List<Map>)value.get("attrs");
      if (attrs == null) {
        LOG.info(value.fullPath() + " has no attributes");
        return;
      }
      long fsByteOffset = ((Number)value.get("fs_byte_offset")).longValue();
      long fsBlockSize = ((Number)value.get("fs_block_size")).longValue();
      for (Map attribute: attrs) {
        long flags = ((Number)attribute.get("flags")).longValue();
        long type = ((Number)attribute.get("type")).longValue();
        if ((flags & 0x03) > 0 && (type & 0x81) > 0) { // flags & type indicate this attribute has nonresident data
          Object nrds = attribute.get("nrd_runs");
          if (nrds == null) {
            LOG.warn(value.fullPath() + " had an nrd attr with null runs");
            return;
          }
          List<Map<String, Object>> runs = (List<Map<String,Object>>)nrds;
          if (setExtents(runs, fsByteOffset, fsBlockSize) && !Extents.isEmpty()) {
            Offset.set(((Number)Extents.get(0).get("addr")).longValue());
            Output.put("size", value.get("size"));
            Output.put("fp", value.fullPath());
            Output.put("id", key.toString());
            JsonOutput.set(Output);
            LOG.info(JsonOutput.toString());
            context.write(Offset, JsonOutput);
            break; // take the first one we get... FIXME someday
          }
        }
      }
    }
    catch (ClassCastException e) {
      LOG.warn(errorString(key, value, e));
      e.printStackTrace();
    }
    catch (NullPointerException e) {
      LOG.warn(errorString(key, value, e));
      e.printStackTrace();
    }
    catch (Exception e) {
      LOG.warn(errorString(key, value, e));
      e.printStackTrace();
    }
  }
}
