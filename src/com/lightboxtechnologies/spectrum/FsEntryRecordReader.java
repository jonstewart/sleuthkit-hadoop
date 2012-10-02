package com.lightboxtechnologies.spectrum;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.commons.codec.binary.Hex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FsEntryRecordReader
                          extends RecordReader<ImmutableHexWritable, FsEntry> {
  private static final Log LOG = LogFactory.getLog(FsEntryRecordReader.class);

  private final RecordReader<ImmutableBytesWritable, Result> TblReader;
  private Result Cur;
  private final ImmutableHexWritable Key;
  private final FsEntry Value;
  private FsEntryRowFilter Filter;

  private final byte[] First;
  private final byte[] Last;

  public FsEntryRecordReader(RecordReader<ImmutableBytesWritable, Result> rr, byte[] first, byte[] last, byte[] imgID) {
    TblReader = rr;
    Key = new ImmutableHexWritable();
    Value = new FsEntry();
    if (imgID != null && imgID.length > 0) {
      Filter = new FsEntryRowFilter(imgID);
    }

    First = first;
    Last = last;
    LOG.info("Scanning range " + First.toString() + " to " + Last.toString());
  }

  public void close() throws IOException {
    TblReader.close();
  }

  public ImmutableHexWritable getCurrentKey() throws IOException, InterruptedException {
    Cur = TblReader.getCurrentValue();
    Key.set(Cur.getRow(), 0, Cur.getRow().length);
    return Key;
  }

  public FsEntry getCurrentValue() throws IOException, InterruptedException {
    final Map<byte[], byte[]> family = Cur.getFamilyMap(HBaseTables.ENTRIES_COLFAM_B);
    FsEntryHBaseCommon.populate(family, Value, Value.getStreams());
    return Value;
  }

  public float getProgress() throws IOException, InterruptedException {
    return TblReader.getProgress();
  }

  public void initialize(InputSplit split, TaskAttemptContext ctx) throws IOException, InterruptedException {
    TblReader.initialize(split, ctx);
    Value.setFileSystem(FileSystem.get(ctx.getConfiguration()));
  }

  public boolean nextKeyValue() throws IOException, InterruptedException {
    while (TblReader.nextKeyValue()) {
      Cur = TblReader.getCurrentValue();

      if (Filter == null || Filter.compareTo(Cur.getRow()) == 0) {
        return true;
      }

      if (TblReader instanceof TableRecordReader) {
//          throw new RuntimeException("WFT!");

        final byte[] jumpkey =
          FsEntryUtils.nextFsEntryKey(getCurrentKey().get());
        LOG.info("jumpkey is " + jumpkey.toString());

        if (Bytes.compareTo(jumpkey, Last) < 0) {
          ((TableRecordReader) TblReader).restart(jumpkey);
          Cur = TblReader.getCurrentValue();
          LOG.info("jumped to " + Hex.encodeHexString(jumpkey));
          return true;
        }
      }
    }
    LOG.info("Reached end of scan, last key was " + Key.toString());
    return false;
  }
}
