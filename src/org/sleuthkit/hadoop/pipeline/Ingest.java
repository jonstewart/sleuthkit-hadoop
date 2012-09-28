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

import com.lightboxtechnologies.spectrum.ExtractData;
import com.lightboxtechnologies.spectrum.ExtentsExtractor;
import com.lightboxtechnologies.spectrum.JsonImport;

public class Ingest {
  public static void main(String[] argv) throws Exception {
    if (argv.length != 4) {
      System.err.println("Usage: Ingest <image_hash_id> <image.dd> <image.json> <friendlyName>");
      System.exit(2);
    }
    final String imgID = argv[0];
    final String image = argv[1];
    final String jsonImg = argv[2];
    final String friendlyName = argv[3];
    final String extents = "/texaspete/data/" + imgID + "/extents";
    if (0 == new JsonImport().run(new String[]{ jsonImg, imgID, friendlyName })
      && 0 == new ExtentsExtractor().run(new String[]{ imgID, friendlyName, extents })
      && 0 == ExtractData.run(imgID, friendlyName, extents + "/part-r-00000", image, null))
    {
      System.exit(0);
    }
    else {
      System.exit(1);
    }
  }
}
