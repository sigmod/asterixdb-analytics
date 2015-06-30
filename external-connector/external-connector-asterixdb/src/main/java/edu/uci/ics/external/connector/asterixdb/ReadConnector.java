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

package edu.uci.ics.external.connector.asterixdb;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;

import edu.uci.ics.external.connector.api.FilePartition;
import edu.uci.ics.external.connector.api.IFieldReadConverterFactory;
import edu.uci.ics.external.connector.api.IReadConnector;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class ReadConnector implements IReadConnector {

    @Override
    public List<FilePartition> getPaths(Object resourceIdentifier) {
        List<FilePartition> pathList = new ArrayList<FilePartition>();
        String[] nameStrs = (String[]) resourceIdentifier;
        if (nameStrs.length < 3) {
            return pathList;
        }
        String urlStr = nameStrs[0];
        String dataverseName = nameStrs[1];
        String datasetName = nameStrs[2];
        return null;
    }

    @Override
    public IFieldReadConverterFactory getFieldConverterFactory() {
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IOperatorDescriptor getReadOperatorDescriptor(JobSpecification jobSpec, List<Path> outputPaths,
            Object parameter) {
        // load Asterix file splits
        return null;
    }

}
