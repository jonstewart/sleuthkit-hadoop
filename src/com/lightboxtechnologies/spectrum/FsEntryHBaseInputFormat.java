/*
src/com/lightboxtechnologies/spectrum/FsEntryHBaseInputFormat.java

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

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.DecoderException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;

import com.lightboxtechnologies.io.IOUtils;

import org.sleuthkit.hadoop.core.SKMapper;

public class FsEntryHBaseInputFormat extends InputFormat implements Configurable {
  private static final Log LOG = LogFactory.getLog(FsEntryHBaseInputFormat.class);

  private byte[] ImageID;
  private TableInputFormat TblInput;
  private HTable ImagesTbl;

  public FsEntryHBaseInputFormat() {
    TblInput = new TableInputFormat();
  }

  static public void setupJob(Job job, String deviceID) throws IOException, DecoderException {
    Scan scan = new Scan();
    scan.addFamily(HBaseTables.ENTRIES_COLFAM_B);
/*
    if (deviceID != null && deviceID.length() > 0) {
      byte[] imgID = Hex.decodeHex(deviceID.toCharArray());
      FsEntryRowFilter keyFilt = new FsEntryRowFilter(imgID);
      RowFilter filt = new RowFilter(CompareFilter.CompareOp.EQUAL, keyFilt);
      scan.setFilter(filt);
    }
*/
    HBaseConfiguration.addHbaseResources(job.getConfiguration());
    job.getConfiguration().set(TableInputFormat.INPUT_TABLE, HBaseTables.ENTRIES_TBL);
    job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));
    job.getConfiguration().set(SKMapper.ID_KEY, deviceID);
    LOG.info("hbase.zookeeper.quorum:" + job.getConfiguration().get("hbase.zookeeper.quorum"));
  }

  static String convertScanToString(Scan scan) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dos = null;
    try {
      dos = new DataOutputStream(out);
      scan.write(dos);
      dos.close();
      return Base64.encodeBytes(out.toByteArray());
    }
    finally {
      IOUtils.closeQuietly(dos);
    }
  }

  public Configuration getConf() {
    return TblInput.getConf();
  }

  public void setConf(Configuration c) {
    String tblname = c.get(TableInputFormat.INPUT_TABLE);
    if (tblname != null) {
      LOG.info("Table name from conf was: " + tblname);
    }
    else {
      LOG.error("Table name was null");
    }

    final Configuration conf = HBaseConfiguration.create(c);
    LOG.info("hbase.zookeeper.quorum:" + conf.get("hbase.zookeeper.quorum"));
    TblInput.setConf(conf);
    String id = conf.get(SKMapper.ID_KEY);
    if (id != null) {
      LOG.info("scan is for image id " + id);
      // we catch and rethrow because Configurable.setConf() doesn't throw
      try {
        ImageID = Hex.decodeHex(id.toCharArray());
      }
      catch (DecoderException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      ImagesTbl = new HTable(conf, HBaseTables.IMAGES_TBL_B);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<InputSplit> getSplits(JobContext ctx) throws IOException {
    return TblInput.getSplits(ctx);
  }

  public RecordReader<ImmutableHexWritable, FsEntry> createRecordReader(InputSplit split, TaskAttemptContext ctx) throws IOException {
    RecordReader<ImmutableHexWritable,FsEntry> outer = null;
    RecordReader<ImmutableBytesWritable,Result> inner = null;
    try {
      inner = TblInput.createRecordReader(split, ctx);
      
      final Scan scan = TblInput.getScan();
      final byte[] first = scan.getStartRow();
      final byte[] last = scan.getStopRow();     

      outer = new FsEntryRecordReader(inner, first, last, ImageID, ImagesTbl);
      return outer;
    }
    finally {
      // make sure inner is closed if outer creation throws
      if (outer == null) {
        IOUtils.closeQuietly(inner);
      }
    }
  }
}
