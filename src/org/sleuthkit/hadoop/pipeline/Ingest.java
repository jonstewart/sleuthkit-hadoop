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


package org.sleuthkit.hadoop.pipeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.lightboxtechnologies.spectrum.ExtractData;
import com.lightboxtechnologies.spectrum.ExtentsExtractor;
import com.lightboxtechnologies.spectrum.JsonImport;

public class Ingest extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: Ingest <image_hash_id> <image.dd> <image.json> <friendlyName>");
      return 2;
    }

    final String imgID = args[0];
    final String image = args[1];
    final String jsonImg = args[2];
    final String friendlyName = args[3];
    final String extents = "/texaspete/data/" + imgID + "/extents";   

    return 
        ToolRunner.run(
          HBaseConfiguration.create(),
          new JsonImport(),
          new String[]{ jsonImg, imgID, friendlyName }
        ) == 0
      &&
        ToolRunner.run(
          new Configuration(),
          new ExtentsExtractor(),
          new String[]{ imgID, friendlyName, extents }
        ) == 0
      &&
        ToolRunner.run(
          HBaseConfiguration.create(),
          new ExtractData(),
          new String[]{ imgID, friendlyName, extents + "/part-r-00000", image }
        ) == 0
      ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(
      ToolRunner.run(new Configuration(), new Ingest(), args)
    );
  }
}
