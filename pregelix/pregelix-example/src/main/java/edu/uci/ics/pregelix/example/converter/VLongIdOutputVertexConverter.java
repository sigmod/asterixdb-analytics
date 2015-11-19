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

package edu.uci.ics.pregelix.example.converter;

import java.io.DataOutput;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.pregelix.api.converter.VertexOutputConverter;
import edu.uci.ics.pregelix.api.graph.Vertex;

public class VLongIdOutputVertexConverter implements VertexOutputConverter {

    // Field indexes.
    private int vertexIdFieldIndex = -1;
    private int vertexValueFieldIndex = -1;

    // Type tags.
    private ATypeTag vertexIdTypeTag;
    private ATypeTag vertexValueTypeTag;

    // ArrayBacked storage
    private ArrayBackedValueStorage vertexIdStorage = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage vertexValueStorage = new ArrayBackedValueStorage();

    @Override
    public void open(ARecordType recordType) {
        setFieldPermutation(recordType);
    }

    @Override
    public void convert(Writable vid, DataOutput dataOutput) throws HyracksDataException {
        outputVertexId(vid, dataOutput);
    }

    private void outputVertexId(Writable vid, DataOutput dataOutput) throws HyracksDataException {
        long id = ((VLongWritable) vid).get();
        try {
            // Writes the type tag
            dataOutput.writeByte(vertexIdTypeTag.serialize());
            switch (vertexIdTypeTag) {
                case INT8: {
                    dataOutput.writeByte((int) id);
                    break;
                }
                case INT16: {
                    dataOutput.writeShort((int) id);
                    break;
                }
                case INT32: {
                    dataOutput.writeInt((int) id);
                    break;
                }
                case INT64: {
                    dataOutput.writeLong(id);
                    break;
                }
                case STRING: {
                    dataOutput.writeUTF(Long.toString(id));
                    break;
                }
                default: {
                    throw new NotImplementedException("No printer for type " + vertexIdTypeTag);
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private void outputVertexValue(Writable value, DataOutput dataOutput) throws HyracksDataException {
        try {
            // Writes the type tag
            dataOutput.writeByte(vertexValueTypeTag.serialize());
            switch (vertexValueTypeTag) {
                case INT8: {
                    long v = ((VLongWritable) value).get();
                    dataOutput.writeByte((int) v);
                    break;
                }
                case INT16: {
                    long v = ((VLongWritable) value).get();
                    dataOutput.writeShort((int) v);
                    break;
                }
                case INT32: {
                    long v = ((VLongWritable) value).get();
                    dataOutput.writeInt((int) v);
                    break;
                }
                case INT64: {
                    long v = ((VLongWritable) value).get();
                    dataOutput.writeLong(v);
                    break;
                }
                case FLOAT: {
                    float v = ((FloatWritable) value).get();
                    dataOutput.writeFloat(v);
                    break;
                }
                case DOUBLE: {
                    double v = ((DoubleWritable) value).get();
                    dataOutput.writeDouble(v);
                    break;
                }
                case STRING: {
                    String v = ((Text) value).toString();
                    dataOutput.writeUTF(v);
                    break;
                }
                default: {
                    throw new NotImplementedException("No printer for type " + vertexIdTypeTag);
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void convert(Vertex vertex, RecordBuilder recordBuilder) throws HyracksDataException {
        // Clears the temp storage.
        vertexIdStorage.reset();
        vertexValueStorage.reset();

        // Builds the temp storage.
        outputVertexId(vertex.getVertexId(), vertexIdStorage.getDataOutput());
        outputVertexValue(vertex.getVertexValue(), vertexValueStorage.getDataOutput());

        // Adds fields into the record builder.
        recordBuilder.addField(vertexIdFieldIndex, vertexIdStorage);
        recordBuilder.addField(vertexValueFieldIndex, vertexValueStorage);
    }

    @Override
    public void close() {

    }

    private void setFieldPermutation(ARecordType recordType) {
        IAType[] fieldTypes = recordType.getFieldTypes();
        for (int i = 0; i < fieldTypes.length; i++) {
            if (vertexIdFieldIndex >= 0 && vertexValueFieldIndex >= 0) {
                break;
            }
            if (vertexIdFieldIndex == -1 && isIntegerType(fieldTypes[i].getTypeTag())) {
                vertexIdFieldIndex = i;
            } else if (vertexValueFieldIndex == -1 && isPrimitiveType(fieldTypes[i].getTypeTag())) {
                vertexValueFieldIndex = i;
            }
        }
        vertexIdTypeTag = fieldTypes[vertexIdFieldIndex].getTypeTag();
        vertexValueTypeTag = fieldTypes[vertexValueFieldIndex].getTypeTag();
    }

    private boolean isIntegerType(ATypeTag typeTag) {
        return typeTag == ATypeTag.INT8 || typeTag == ATypeTag.INT16 || typeTag == ATypeTag.INT32
                || typeTag == ATypeTag.INT64 || typeTag == ATypeTag.STRING;
    }

    private boolean isCollectionType(ATypeTag typeTag) {
        return typeTag == ATypeTag.ORDEREDLIST || typeTag == ATypeTag.UNORDEREDLIST;
    }

    private boolean isRecordType(ATypeTag typeTag) {
        return typeTag == ATypeTag.RECORD;
    }

    private boolean isPrimitiveType(ATypeTag typeTag) {
        return !isCollectionType(typeTag) && !isRecordType(typeTag);
    }

}
