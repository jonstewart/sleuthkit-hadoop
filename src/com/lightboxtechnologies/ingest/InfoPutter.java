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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.commons.io.IOUtils;

import com.lightboxtechnologies.spectrum.HBaseTables;

/**
 * Stuff fsrip info output into the images table in HBase.
 *
 * @author Joel Uckelman
 */
public class InfoPutter extends Configured implements Tool {
  public int run(String[] args) throws IOException {
    if (args.length != 3) {
      System.err.println("Usage: InfoPutter <imageID> <friendly_name> <pathToImageOnHDFS>");
      return 1;
    }

    final Configuration conf = getConf();

    final String imageID = args[0];
    final String friendlyName = args[1];
    final String imgPath = args[2];

    HTable imgTable = null;

    try {
      imgTable = HBaseTables.summon(
        conf, HBaseTables.IMAGES_TBL_B, HBaseTables.IMAGES_COLFAM_B
      );

      // check whether the image ID is in the images table
      final byte[] hash = Bytes.toBytes(imageID);

      final Get get = new Get(hash);
      final Result result = imgTable.get(get);

      if (result.isEmpty()) {
        // row does not exist, add it
      
        final byte[] friendly_col = "friendly_name".getBytes();
        final byte[] json_col = "json".getBytes();
        final byte[] path_col = "img_path".getBytes();

        final byte[] friendly_b = friendlyName.getBytes();
        final byte[] json_b = IOUtils.toByteArray(System.in);
        final byte[] path_b = imgPath.getBytes(); 

        final Put put = new Put(hash);
        put.add(HBaseTables.IMAGES_COLFAM_B, friendly_col, friendly_b);
        put.add(HBaseTables.IMAGES_COLFAM_B, json_col, json_b);
        put.add(HBaseTables.IMAGES_COLFAM_B, path_col, path_b);

        imgTable.put(put);

        return 0;
      }
      else {
        // row exists, fail!
        return 1;
      }
    }
    finally {
      if (imgTable != null) {
        imgTable.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(
      ToolRunner.run(HBaseConfiguration.create(), new InfoPutter(), args)
    );
  }
}
