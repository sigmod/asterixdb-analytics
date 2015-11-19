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
package edu.uci.ics.pregelix.runtime.function;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.FrameTupleUtils;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunction;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;

@SuppressWarnings("rawtypes")
public class ExtractLiveVertexIdFunctionFactory implements IUpdateFunctionFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public IUpdateFunction createFunction() {
        return new IUpdateFunction() {
            // for writing intermediate data
            private final ArrayTupleBuilder alive = new ArrayTupleBuilder(2);

            // for writing out to alive message channel
            private IFrameWriter writerAlive;
            private IFrameFieldAppender appenderAlive;
            private IFrame frameAlive;

            private MsgList dummyMessageList = new MsgList();
            private Vertex vertex;

            @Override
            public void open(IHyracksTaskContext ctx, RecordDescriptor rd, IFrameWriter... writers)
                    throws HyracksDataException {
                this.writerAlive = writers[0];
                this.frameAlive = new VSizeFrame(ctx);
                this.appenderAlive = new FrameFixedFieldTupleAppender(1);
                this.appenderAlive.reset(frameAlive, true);
            }

            @Override
            public void process(Object[] tuple) throws HyracksDataException {
                try {
                    // vertex Id, vertex
                    alive.reset();
                    vertex = (Vertex) tuple[1];
                    if (!vertex.isHalted()) {
                        alive.reset();
                        DataOutput outputAlive = alive.getDataOutput();
                        vertex.getVertexId().write(outputAlive);
                        alive.addFieldEndOffset();
                        dummyMessageList.write(outputAlive);
                        alive.addFieldEndOffset();
                        FrameTupleUtils.flushTuple(appenderAlive, alive, writerAlive);
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }

            }

            @Override
            public void close() throws HyracksDataException {
                FrameTupleUtils.flushTuplesFinal(appenderAlive, writerAlive);
            }

            @Override
            public void update(ITupleReference tupleRef, ArrayTupleBuilder cloneUpdateTb, IIndexCursor cursor)
                    throws HyracksDataException {

            }
        };
    }
}
