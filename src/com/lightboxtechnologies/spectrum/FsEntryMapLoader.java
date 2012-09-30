package com.lightboxtechnologies.spectrum;

import java.io.IOException;
import java.security.MessageDigest;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;

import org.sleuthkit.hadoop.core.SKMapper;

public class FsEntryMapLoader
              extends SKMapper<Object, Text, ImmutableBytesWritable, FsEntry> {

  private final FsEntry Entry = new FsEntry();
  private final ImmutableBytesWritable Id =
    new ImmutableBytesWritable(new byte[FsEntryUtils.ID_LENGTH]);
  private final MessageDigest Hasher = FsEntryUtils.getHashInstance("MD5");

  @Override
  protected void map(Object key, Text value, Context context)
                                     throws IOException, InterruptedException {
    if (Entry.parseJson(value.toString())) {
      // set the ID re-using the byte array in Id
      FsEntryUtils.makeFsEntryKey(
        Id.get(),
        getImageID(),
        ((String) Entry.get("path")).getBytes(),
        (Integer) Entry.get("dirIndex"),
        Hasher
      );

      context.write(Id, Entry);
    }
  }
}
