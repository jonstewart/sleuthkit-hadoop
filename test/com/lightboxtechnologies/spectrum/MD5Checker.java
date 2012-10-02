package com.lightboxtechnologies.spectrum;

import java.io.IOException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.sleuthkit.hadoop.core.SKJobFactory;
import org.sleuthkit.hadoop.core.SKMapper;

public class MD5Checker extends Configured implements Tool {
  public static class Mapper
                  extends SKMapper<ImmutableHexWritable, FsEntry, Text, Text> {
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(ImmutableHexWritable key, FsEntry value, Context context) throws IOException, InterruptedException {
      outKey.set(value.fullPath());
      outVal.set(Hex.encodeHexString((byte[]) value.get("md5")));
      context.write(outKey, outVal);
    }
  }

  public int run(String[] args) throws ClassNotFoundException, DecoderException, IOException, InterruptedException {

    final String imageID = args[0];
    final String friendlyName = args[1];

    final Configuration conf = getConf();

    final Job job = SKJobFactory.createJobFromConf(
      imageID, friendlyName, "MD5Checker", conf
    );

    job.setJarByClass(MD5Checker.class);
    job.setMapperClass(MD5Checker.Mapper.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(FsEntryHBaseInputFormat.class);
    FsEntryHBaseInputFormat.setupJob(job, imageID);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path("md5checker"));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(
      ToolRunner.run(HBaseConfiguration.create(), new MD5Checker(), args)
    );
  }
}
