package com.lightboxtechnologies.spectrum;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;

import java.io.IOException;

public class FsEntryHBaseWriter
                        extends RecordWriter<ImmutableBytesWritable, FsEntry> {
  private HTable Table;
  final private byte[] ColFam;

  FsEntryHBaseWriter(HTable tbl, byte[] colFam) {
    Table = tbl;
    ColFam = colFam;
  }

  public void write(ImmutableBytesWritable key, FsEntry entry)
                                                           throws IOException {
    final Put p = FsEntryPut.create(key.get(), entry, ColFam);
    if (!p.isEmpty()) {
      Table.put(p);
    }
  }

  public void close(TaskAttemptContext ctx) {
    Table = null;
  }
}
