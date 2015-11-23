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
package edu.uci.ics.pregelix.dataflow;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.pregelix.dataflow.state.MaterializerTaskState;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

public class MaterializingReadOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final boolean removeIterationState;
    private final String jobId;
    private final int iteration;

    public MaterializingReadOperatorDescriptor(JobSpecification spec, RecordDescriptor recordDescriptor,
            boolean removeIterationState, String jobId, int iteration) {
        super(spec, 1, 1);
        this.removeIterationState = removeIterationState;
        this.jobId = jobId;
        this.iteration = iteration - 1;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private IFrame frame = new VSizeFrame(ctx);
            private boolean complete = false;

            @Override
            public void open() throws HyracksDataException {

            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (!complete) {
                    MaterializerTaskState state = (MaterializerTaskState) IterationUtils.getIterationState(ctx, jobId,
                            partition, iteration);
                    RunFileReader in = state.getRunFileWriter().createReader();
                    writer.open();
                    try {
                        in.open();
                        while (in.nextFrame(frame)) {
                            //frame.getBuffer().flip();
                            writer.nextFrame(frame.getBuffer());
                            //frame.getBuffer().clear();
                        }
                        in.close();
                    } catch (Exception e) {
                        writer.fail();
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                /**
                 * remove last iteration's state
                 */
                if (removeIterationState) {
                    IterationUtils.removeIterationState(ctx, jobId, partition, iteration);
                }
                writer.close();
                complete = true;
            }
        };
    }
}