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

import java.io.DataInput;
import java.io.DataInputStream;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.external.connector.asterixdb.api.IWriteConverter;
import edu.uci.ics.external.connector.asterixdb.api.IWriteConverterFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.pregelix.api.converter.VertexOutputConverter;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.api.util.ResetableByteArrayInputStream;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;

public class WriteConverterFactory implements IWriteConverterFactory {
    private static final long serialVersionUID = 1L;
    private final IConfigurationFactory confFactory;

    public WriteConverterFactory(IConfigurationFactory confFactory) {
        this.confFactory = confFactory;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IWriteConverter getFieldWriteConverter() throws HyracksDataException {
        final Configuration conf = confFactory.createConfiguration();
        final Vertex vertex = BspUtils.createVertex(conf);
        final VertexOutputConverter outputConverter = BspUtils.createVertexOutputConverter(conf);
        final ResetableByteArrayInputStream inputStream = new ResetableByteArrayInputStream();
        final DataInput dataInput = new DataInputStream(inputStream);
        final RecordBuilder recordBuilder = new RecordBuilder();

        return new IWriteConverter() {
            private boolean first = true;

            @Override
            public void convert(ARecordType recordType, byte[] data, int start, int len, ArrayTupleBuilder outputTb)
                    throws HyracksDataException {
                try {
                    if (first) {
                        first = false;
                        recordBuilder.reset(recordType);
                    }
                    inputStream.setByteArray(data, start);
                    vertex.readFields(dataInput);
                    recordBuilder.init();
                    outputConverter.convert(recordType, vertex, recordBuilder);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

        };
    }
}
