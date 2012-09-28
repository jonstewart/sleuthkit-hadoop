/*
   Copyright 2011 Basis Technology Corp.

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

package org.sleuthkit.hadoop.match;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 * Sums the numbmer of times a given item has been found through grep
 * searching. Outputs that data to a JSON array. This is then packaged
 * into an output report.
 */
public class GrepCountReducer extends Reducer<LongWritable, LongWritable, NullWritable, Text> {
  String regexes[];

  final ByteArrayOutputStream buf = new ByteArrayOutputStream();
  JsonGenerator gen;

  @Override
  protected void setup(Context context) throws IOException {
    // TODO: We could make sure that we only have one instance of this
    // here, since that's all I'm supporting at the moment.
    regexes = context.getConfiguration().get("mapred.mapper.regex").split("\n");

    gen = new JsonFactory().createJsonGenerator(buf, JsonEncoding.UTF8);
    gen.writeStartArray();
  }

  @SuppressWarnings("unchecked")
  @Override 
  protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException {
    gen.writeStartObject();

    int sum = 0;
    for (LongWritable value : values) {
      sum += value.get();
    }
  
    gen.writeNumberField("a", key.get());
    gen.writeNumberField("n", sum);
    gen.writeStringField("kw", regexes[(int) key.get()]);

    gen.writeEndObject();
  }

  @Override
  protected void cleanup(Context context)
                                   throws IOException, InterruptedException {
    gen.writeEndArray();
    gen.close();
    context.write(NullWritable.get(), new Text(buf.toString("UTF-8")));
  }
}
