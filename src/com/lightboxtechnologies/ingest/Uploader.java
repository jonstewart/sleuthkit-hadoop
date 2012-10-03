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

package com.lightboxtechnologies.ingest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.DecoderException;

import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.lightboxtechnologies.io.IOUtils;
import com.lightboxtechnologies.spectrum.FsEntryUtils;

public class Uploader extends Configured implements Tool {
  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: Uploader <dest path>");
      System.err.println("Writes data to HDFS path from stdin");
      return 2;
    }

    final String dst = args[0];

    final Configuration conf = getConf();

    final MessageDigest hasher = FsEntryUtils.getHashInstance("MD5");
    final DigestInputStream hashedIn = new DigestInputStream(System.in, hasher);

    final FileSystem fs = FileSystem.get(conf);
    final Path path = new Path(dst);
    final FSDataOutputStream outFile = fs.create(path, true);

    IOUtils.copyLarge(hashedIn, outFile, new byte[1024 * 1024]);
    System.out.println(Hex.encodeHexString(hasher.digest()));
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new Uploader(), args));
  }
}
