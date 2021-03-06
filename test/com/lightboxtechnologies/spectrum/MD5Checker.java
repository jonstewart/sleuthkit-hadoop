package com.lightboxtechnologies.spectrum;

import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

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

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lightboxtechnologies.io.IOUtils;

import org.sleuthkit.hadoop.core.SKJobFactory;
import org.sleuthkit.hadoop.core.SKMapper;

public class MD5Checker extends Configured implements Tool {
  public static class Mapper
                  extends SKMapper<ImmutableHexWritable, FsEntry, Text, Text> {
    private static final Log LOG =
      LogFactory.getLog(MD5Checker.class.getName());

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(ImmutableHexWritable key, FsEntry value, Context context) throws IOException, InterruptedException {
      final byte[] actual_md5 = (byte[]) value.get("md5");
      if (actual_md5 == null) {
        return;
      }

      final MessageDigest hasher = FsEntryUtils.getHashInstance("MD5");
      final DigestInputStream in =
        new DigestInputStream(value.getInputStream(), hasher);

      IOUtils.copyLarge(
        in,
        NullOutputStream.NULL_OUTPUT_STREAM,
        new byte[1024 * 1024]
      );

      final byte[] expected_md5 = hasher.digest();

      if (!Arrays.equals(actual_md5, expected_md5)) {
        LOG.error(
          value.fullPath() + ": " +
          Hex.encodeHexString(actual_md5) + " != " +
          Hex.encodeHexString(expected_md5)
        );
      }

      outKey.set(value.fullPath());
      outVal.set(
        Hex.encodeHexString(actual_md5) + ' ' +
        Hex.encodeHexString(expected_md5)
      );
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
