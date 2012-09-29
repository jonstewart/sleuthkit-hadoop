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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.OutputCommitter;

import java.io.IOException;

public class FsEntryHBaseOutputFormat extends OutputFormat {

  private static final Log LOG = LogFactory.getLog(FsEntryHBaseInputFormat.class);

  public static class NullOutputCommitter extends OutputCommitter {
    public void 	abortTask(TaskAttemptContext taskContext) {}
    public void 	commitTask(TaskAttemptContext taskContext) {}
    public boolean 	needsTaskCommit(TaskAttemptContext taskContext) { return false; }
    public void 	setupJob(JobContext jobContext) {}
    public void 	setupTask(TaskAttemptContext taskContext) {}
  }

  public OutputCommitter getOutputCommitter(TaskAttemptContext ctx) {
    return new NullOutputCommitter();
  }

  public void	checkOutputSpecs(JobContext context) {}

  public static HTable getHTable(TaskAttemptContext ctx, byte[] colFam) throws IOException {
    final Configuration conf = HBaseConfiguration.create(ctx.getConfiguration());
    LOG.info("hbase.zookeeper.quorum:" + conf.get("hbase.zookeeper.quorum"));
    final String tblName = conf.get(HBaseTables.ENTRIES_TBL_VAR, HBaseTables.ENTRIES_TBL);

    return HBaseTables.summon(
      conf, tblName.getBytes(), HBaseTables.ENTRIES_COLFAM_B
    );
  }

  public RecordWriter<ImmutableBytesWritable, FsEntry> getRecordWriter(TaskAttemptContext ctx) throws IOException {
    return new FsEntryHBaseWriter(getHTable(ctx, HBaseTables.ENTRIES_COLFAM_B), HBaseTables.ENTRIES_COLFAM_B);
  }
}
