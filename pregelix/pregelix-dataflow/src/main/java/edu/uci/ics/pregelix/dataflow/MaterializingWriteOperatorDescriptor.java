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

import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.pregelix.dataflow.context.RuntimeContext;
import edu.uci.ics.pregelix.dataflow.state.MaterializerTaskState;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

public class MaterializingWriteOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final static int MATERIALIZER_ACTIVITY_ID = 0;
    private final String jobId;
    private final int iteration;

    public MaterializingWriteOperatorDescriptor(JobSpecification spec, RecordDescriptor recordDescriptor, String jobId,
            int iteration) {
        super(spec, 1, 1);
        this.jobId = jobId;
        this.iteration = iteration;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        MaterializerActivityNode ma = new MaterializerActivityNode(new ActivityId(odId, MATERIALIZER_ACTIVITY_ID));

        builder.addActivity(this, ma);
        builder.addSourceEdge(0, ma, 0);
        builder.addTargetEdge(0, ma, 0);
    }

    private final class MaterializerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MaterializerActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private MaterializerTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    /** remove last iteration's state */
                    IterationUtils.removeIterationState(ctx, jobId, partition, iteration);
                    state = new MaterializerTaskState(ctx.getJobletContext().getJobId(), new TaskId(getActivityId(),
                            partition));
                    INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
                    RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
                    FileReference file = context.createManagedWorkspaceFile(jobId);
                    state.setRunFileWriter(new RunFileWriter(file, ctx.getIOManager()));
                    state.getRunFileWriter().open();
                    writer.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.getRunFileWriter().nextFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.getRunFileWriter().close();
                    /**
                     * set iteration state
                     */
                    IterationUtils.setIterationState(ctx, jobId, partition, iteration, state);
                    writer.close();
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }
    }
}