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

package edu.uci.ics.pregelix.dataflow.std.util;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IndexException;

public class CopyUpdateUtil {

    public static void copyUpdate(SearchKeyTupleReference tempTupleReference, ITupleReference indexTuple,
            UpdateBuffer updateBuffer, ArrayTupleBuilder cloneUpdateTb, IIndexAccessor indexAccessor,
            IIndexCursor cursor, RangePredicate rangePred, boolean scan, StorageType type) throws HyracksDataException,
            IndexException {
        if (cloneUpdateTb.getSize() > 0) {
            if (!updateBuffer.appendTuple(cloneUpdateTb)) {
                //release the cursor/latch
                cursor.close();
                //batch update
                updateBuffer.updateIndex(indexAccessor);
                //try append the to-be-updated tuple again
                if (!updateBuffer.appendTuple(cloneUpdateTb)) {
                    throw new HyracksDataException("cannot append tuple builder!");
                }

                //search again and recover the cursor to the exact point as the one before it is closed

                if (indexTuple == null) {
                    // return if the cursor already reached the end
                    indexTuple = cursor.getTuple();
                    if (indexTuple == null) {
                        return;
                    }
                }
                tempTupleReference.reset(indexTuple.getFieldData(0), indexTuple.getFieldStart(0),
                        indexTuple.getFieldLength(0));
                cursor.reset();
                rangePred.setLowKey(tempTupleReference, true);
                if (scan) {
                    rangePred.setHighKey(null, true);
                } else {
                    rangePred.setHighKey(tempTupleReference, true);
                }
                indexAccessor.search(cursor, rangePred);
                if (cursor.hasNext()) {
                    cursor.next();
                }
            }
            cloneUpdateTb.reset();
        }
    }
}
