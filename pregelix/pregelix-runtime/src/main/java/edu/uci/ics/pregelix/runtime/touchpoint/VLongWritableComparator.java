/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.pregelix.runtime.touchpoint;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparator;

import edu.uci.ics.pregelix.api.util.SerDeUtils;

/** A Comparator optimized for VLongWritable. */
public class VLongWritableComparator extends WritableComparator {

    public VLongWritableComparator() {
        super(VLongWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            long thisValue = SerDeUtils.readVLong(b1, s1, l1);
            long thatValue = SerDeUtils.readVLong(b2, s2, l2);
            return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
