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

import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexNonExistentKeyException;

/**
 * The buffer to hold updates.
 * We do a batch update for the B-tree during index search and join so that
 * avoid to open/close cursors frequently.
 */
public class UpdateBuffer {

    private final int sizeLimit;
    private final FrameTupleReference tuple = new FrameTupleReference();
    private final FrameTupleReference lastTuple = new FrameTupleReference();

    private final FrameTupleAppender appender;
    private final IFrameTupleAccessor fta;
    private final IFrame frame;

    public UpdateBuffer(int sizeLimit, IHyracksTaskContext ctx) throws HyracksDataException {
        this.appender = new FrameTupleAppender();
        this.frame = new VSizeFrame(ctx);
        this.appender.reset(frame, true);
        this.sizeLimit = sizeLimit;
        this.fta = new FrameTupleAccessor(new RecordDescriptor(new ISerializerDeserializer[] { null, null }));
    }

    public UpdateBuffer(IHyracksTaskContext ctx) throws HyracksDataException {
        //by default, the update buffer has 64MB pages
        this(64 * 1024 * 1024, ctx);
    }

    public boolean appendTuple(ArrayTupleBuilder tb) throws HyracksDataException {
        if (frame.getFrameSize() > sizeLimit) {
            return false;
        }
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            return false;
        } else {
            return true;
        }
    }

    public void updateIndex(IIndexAccessor bta) throws HyracksDataException, IndexException {
        // batch update
        fta.reset(frame.getBuffer());
        for (int j = 0; j < fta.getTupleCount(); j++) {
            tuple.reset(fta, j);
            try {
                bta.update(tuple);
            } catch (TreeIndexNonExistentKeyException e) {
                // ignore non-existent key exception
                bta.insert(tuple);
            }
        }
        //cleanup the buffer
        appender.reset(frame, true);
    }

    /**
     * return the last updated tuple
     *
     * @throws HyracksDataException
     */
    public ITupleReference getLastTuple() throws HyracksDataException {
        fta.reset(frame.getBuffer());
        int tupleIndex = fta.getTupleCount() - 1;
        if (tupleIndex < 0) {
            return null;
        }
        lastTuple.reset(fta, tupleIndex);
        return lastTuple;
    }
}
