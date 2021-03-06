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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.lightboxtechnologies.io.IOUtils;

public class BlockHashMapper
       extends Mapper<NullWritable,FileSplit,LongWritable,MD5Hash> {

  private static final Log LOG =
    LogFactory.getLog(ExtractMapper.class.getName());

  private static final int BLOCK_SIZE = 512;
  private final byte[] Buffer = new byte[BLOCK_SIZE * 1024];

  private FSDataInputStream ImgFile = null;
  private Path ImgPath = null;

  private final LongWritable  BlockOffset = new LongWritable();

  void openImgFile(Path p, FileSystem fs) throws IOException {
    if (ImgFile != null && p.equals(ImgPath)) {
      return;
    }
    IOUtils.closeQuietly(ImgFile);
    ImgPath = p;
    ImgFile = fs.open(p, 512 * 1024);
  }

  @Override
  protected void map(NullWritable k, FileSplit split, Context context)
                                     throws IOException, InterruptedException {
    final long startOffset = split.getStart(),
                 endOffset = startOffset + split.getLength();
    LOG.info("startOffset = " + startOffset + "; endOffset = " + endOffset);
    context.setStatus("Offset " + startOffset);

    final Configuration conf = context.getConfiguration();
    final FileSystem fs = FileSystem.get(conf);

    openImgFile(split.getPath(), fs);

    long numBlocks = 0;
    long curOffset = startOffset;
    while (curOffset < endOffset) {
      ImgFile.readFully(curOffset, Buffer);
      for (int i = 0; i < Buffer.length; i += BLOCK_SIZE) {
        BlockOffset.set(curOffset + i);
        context.write(BlockOffset, MD5Hash.digest(Buffer, i, BLOCK_SIZE));
        ++numBlocks;
      }
      curOffset += Buffer.length;
    }
    LOG.info("This split had " + numBlocks + " blocks in it");
  }

  @Override
  protected void cleanup(Mapper.Context context) {
    IOUtils.closeQuietly(ImgFile);
  }
}
