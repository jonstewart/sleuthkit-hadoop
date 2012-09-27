/*
src/com/lightboxtechnologies/spectrum/JsonImport.java

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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.sleuthkit.hadoop.core.SKJobFactory;

public class ExtractData {
  protected ExtractData() {}

  public static final Log LOG = LogFactory.getLog(ExtractData.class.getName());

  static void chmodR(FileSystem fs, Path p) throws IOException {
    FsPermission perm = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    FileStatus[] list = fs.listStatus(p);
    for (FileStatus f: list) {
      if (f.isDir()) {
        chmodR(fs, f.getPath());
      }
      fs.setPermission(f.getPath(), perm);
    }
    fs.setPermission(p, perm);
  }

  public static int run(String imageID, String friendlyName, String extentsPath, String image, Configuration conf) throws Exception {
    if (conf == null) {
      conf = HBaseConfiguration.create();
    }
    final Job job = SKJobFactory.createJobFromConf(imageID, friendlyName, "ExtractData", conf);
    job.setJarByClass(ExtractData.class);
    job.setMapperClass(ExtractMapper.class);
    job.setReducerClass(KeyValueSortReducer.class);
    job.setNumReduceTasks(1);

    // job ctor copies the Configuration we pass it, get the real one
    conf = job.getConfiguration();

    conf.setLong("timestamp", System.currentTimeMillis());

    job.setInputFormatClass(RawFileInputFormat.class);
    RawFileInputFormat.addInputPath(job, new Path(image));

    job.setOutputFormatClass(HFileOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);

    conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
    
    FileSystem fs = FileSystem.get(conf);
    Path hfileDir = new Path("/texaspete/ev/tmp", UUID.randomUUID().toString());
    hfileDir = hfileDir.makeQualified(fs);
    LOG.info("Hashes will be written temporarily to " + hfileDir);
    
    HFileOutputFormat.setOutputPath(job, hfileDir);

    final Path extp = new Path(extentsPath);
    final URI extents = extp.toUri();
    LOG.info("extents file is " + extents);

    DistributedCache.addCacheFile(extents, conf);
    conf.set("com.lbt.extentsname", extp.getName());
    // job.getConfiguration().setBoolean("mapred.task.profile", true);
    // job.getConfiguration().setBoolean("mapreduce.task.profile", true);
    boolean result = job.waitForCompletion(true);
    if (result) {
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
      HBaseConfiguration.addHbaseResources(conf);
      loader.setConf(conf);
      LOG.info("Loading hashes into hbase");
      chmodR(fs, hfileDir);
      loader.doBulkLoad(hfileDir, new HTable(conf, HBaseTables.HASH_TBL_B));
//      result = fs.delete(hfileDir, true);
    }
    return result ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 4) {
      System.err.println("Usage: ExtractData <imageID> <friendly_name> <extents_file> <evidence file>");
      System.exit(2);
    }

    System.exit(run(args[0], args[1], args[2], args[3], conf));
  }
}
