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

package edu.uci.ics.pregelix.runtime.converter;

import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.external.connector.asterixdb.api.IReadConverter;
import edu.uci.ics.external.connector.asterixdb.api.IReadConverterFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.pregelix.api.converter.VertexInputConverter;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;

public class ReadConverterFactory implements IReadConverterFactory {
    private static final long serialVersionUID = 1L;
    private final IConfigurationFactory confFactory;

    public ReadConverterFactory(IConfigurationFactory confFactory) {
        this.confFactory = confFactory;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IReadConverter getReadConverter() throws HyracksDataException {
        final Configuration conf = confFactory.createConfiguration();
        final Vertex vertex = BspUtils.createVertex(conf);
        final VertexInputConverter inputConverter = BspUtils.createVertexInputConverter(conf);

        return new IReadConverter() {

            @Override
            public void convert(ARecordType recordType, ARecordPointable recordPointable, ArrayTupleBuilder outputTb)
                    throws HyracksDataException {
                try {
                    // Converts an input AsterixDB record into an vertex object.
                    vertex.reset();
                    inputConverter.convert(recordType, recordPointable, vertex);

                    // Outputs a tuple of <vertexId, vertex>.
                    outputTb.reset();
                    WritableComparable vertexId = vertex.getVertexId();
                    DataOutput dos = outputTb.getDataOutput();
                    vertexId.write(dos);
                    outputTb.addFieldEndOffset();
                    vertex.write(dos);
                    outputTb.addFieldEndOffset();
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

        };
    }
}
