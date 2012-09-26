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

package org.sleuthkit.hadoop.scoring;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;

/** A subclass of ArrayWritable for Bytes. Required by java
 * and hadoop to instantiate BytesWritable with a no-args constructor,
 * so we can use it in sequence files.
 */
public class BytesArrayWritable extends ArrayWritable {
    public BytesArrayWritable() {
        super(BytesWritable.class);
    }
}
