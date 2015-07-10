/*
 * Copyright 2009-2015 by The Regents of the University of California
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

package edu.uci.ics.external.connector.asterixdb.api;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public interface IWriteConverter {

    /**
     * Converts the binary data into AsterixDB format.
     *
     * @param recordType
     *            , record type
     * @param data
     *            , the byte array holds the input data
     * @param start
     *            , the start offset of the byte array
     * @param len
     *            , the length of the byte array
     * @param outputTb
     *            , the output tuple builder
     */
    public void convert(ARecordType recordType, byte[] data, int start, int len, ArrayTupleBuilder outputTb)
            throws HyracksDataException;

}
