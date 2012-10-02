package com.lightboxtechnologies.spectrum;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
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

  private final HTable ImagesTbl;
  private FSDataInputStream DiskImage;
  private FileSystem FS;
  private byte[] CurImage;

  public FsEntryRecordReader(RecordReader<ImmutableBytesWritable, Result> rr, byte[] first, byte[] last, byte[] imgID, HTable imgTable) {
    TblReader = rr;
    Key = new ImmutableHexWritable();
    Value = new FsEntry();
    if (imgID != null && imgID.length > 0) {
      Filter = new FsEntryRowFilter(imgID);
    }

    ImagesTbl = imgTable;

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
    setDiskImageOnValue();
    final Map<byte[], byte[]> family =
      Cur.getFamilyMap(HBaseTables.ENTRIES_COLFAM_B);
    FsEntryHBaseCommon.populate(family, Value, Value.getStreams());
    return Value;
  }

  public float getProgress() throws IOException, InterruptedException {
    return TblReader.getProgress();
  }

  public void initialize(InputSplit split, TaskAttemptContext ctx) throws IOException, InterruptedException {
    TblReader.initialize(split, ctx);
    FS = FileSystem.get(ctx.getConfiguration());
    Value.setFileSystem(FS);
  }

  protected void setDiskImageOnValue() throws IOException {
    final byte[] hash = FsEntryUtils.getImageID(Key.get());
    if (Arrays.equals(CurImage, hash)) {
      return;
    }

    final byte[] path_col = "img_path".getBytes();
    final Get request =
      new Get(hash).addColumn(HBaseTables.IMAGES_COLFAM_B, path_col);
    final Result result = ImagesTbl.get(request);

    if (result.isEmpty()) {
      throw new IOException(
        "image ID " + Hex.encodeHexString(hash) + " not found in images table"
      ); 
    }

    final byte[] path_b =
      result.getValue(HBaseTables.IMAGES_COLFAM_B, path_col);
    
    final FSDataInputStream in = FS.open(new Path(new String(path_b)));
    Value.setDiskImage(in);
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
