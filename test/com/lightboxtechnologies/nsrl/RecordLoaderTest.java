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

package com.lightboxtechnologies.nsrl;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;

import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CharSequenceReader;

/**
 * @author Joel Uckelman
 */
@RunWith(JMock.class)
public class RecordLoaderTest {
  private final Mockery context = new JUnit4Mockery();

  private static final String line = "Jackdaws love my great sphinx of quartz.";
  private static final int limit = 10;

  private LineHandler buildLineHandler(final int limit, final String line)
                                                           throws IOException {
    final LineHandler lh = context.mock(LineHandler.class);

    context.checking(new Expectations() {
      {
        // expect one less than limit, since the first line is discarded
        for (int i = 1; i < limit; ++i) {
          oneOf(lh).handle(with(line), with((long)i));
        }
      }
    });

    return lh;
  }

  private CharSequence buildInput(int limit, String line) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < limit; ++i) sb.append(line).append('\n');
    return sb;
  }

  @Test
  public void loadReader() throws IOException {
    final LineHandler lh = buildLineHandler(limit, line);
    final Reader r = new CharSequenceReader(buildInput(limit, line));

    final RecordLoader loader = new RecordLoader();
    loader.load(r, lh);
  }

  @Test
  public void loadInputStream() throws IOException {
    final LineHandler lh = buildLineHandler(limit, line);
    final RecordLoader loader = new RecordLoader();

    InputStream in = null;
    try {
      in = IOUtils.toInputStream(buildInput(limit, line).toString());
      loader.load(in, lh);
      in.close();
    }
    finally {
      IOUtils.closeQuietly(in);
    }
  }

  @Test
  public void loadFilename() throws IOException {
    final LineHandler lh = buildLineHandler(limit, line);
    final String filename = "test/com/lightboxtechnologies/nsrl/jackdaws";

    final RecordLoader loader = new RecordLoader();
    loader.load(filename, lh);
  }
}
