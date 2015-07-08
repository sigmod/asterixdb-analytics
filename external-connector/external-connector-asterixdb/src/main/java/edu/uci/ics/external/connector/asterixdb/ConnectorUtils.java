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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.external.connector.asterixdb.api.FilePartition;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;

public class ConnectorUtils {

    // Retrieves the type and partition information of the target AsterixDB dataset.
    public static Pair<ARecordType, String[]> retrieveRecordTypeAndPartitions(StorageParameter storageParameter)
            throws Exception {
        HttpClient client = new HttpClient();

        // Create a method instance.
        GetMethod method = new GetMethod(storageParameter.getServiceURL());

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
        method.getParams().setParameter("dataverseName", storageParameter.getDataverseName());
        method.getParams().setParameter("datasetName", storageParameter.getDatasetName());

        try {
            // Executes the method.
            int statusCode = client.executeMethod(method);
            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Method failed: " + method.getStatusLine());
            }

            // Reads the response body.
            byte[] responseBody = method.getResponseBody();

            // Deals with the response.
            JSONTokener tokener = new JSONTokener(new InputStreamReader(new ByteArrayInputStream(responseBody)));
            JSONObject response = new JSONObject(tokener);

            // Extracts record type and file partitions.
            ARecordType recordType = extractRecordType(response);
            List<FilePartition> filePartitions = extractFilePartitions(response);
            String[] locations = getScanLocationConstraints(filePartitions, storageParameter.getIpToNcNames());
            return Pair.of(recordType, locations);
        } catch (HttpException e) {
            System.err.println("Fatal protocol violation: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } catch (IOException e) {
            System.err.println("Fatal transport error: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            // Release the connection.
            method.releaseConnection();
        }
    }

    /**
     * Creates file split provider
     * 
     * @param inputs
     *            List<FilePartition>
     * @return IFileSplitProvider
     */
    public static IFileSplitProvider createFileSplitProvider(StorageParameter storageParameter, List<FilePartition> inputs) {
        FileSplit[] splits = new FileSplit[inputs.size()];
        int i = 0;
        for (FilePartition p : inputs) {
            // make sure the path has a slash at the end
            String path = p.getPath();
            if (!path.endsWith("/")) {
                path += "/";
            }
            String ncName = storageParameter.getIpToNcNames().get(p.getIPAddress());
            splits[i++] = new FileSplit(ncName, path);
        }
        return new ConstantFileSplitProvider(splits);
    }

    // Extracts the record type of a dataset from the AsterixDB REST response.
    private static ARecordType extractRecordType(JSONObject response) throws Exception {
        return (ARecordType) ARecordType.convertFromJSON((JSONObject) response.get("type"));
    }

    // Extracts the file partitions of a dataset from the AsterixDB REST response.
    private static List<FilePartition> extractFilePartitions(JSONObject response) throws JSONException {
        // Converts to FilePartition arrays.
        List<FilePartition> partitions = new ArrayList<FilePartition>();
        JSONArray splits = response.getJSONArray("splits");
        for (int i = 0; i < splits.length(); i++) {
            String ipAddress = ((JSONObject) splits.get(i)).getString("ip");
            String path = ((JSONObject) splits.get(i)).getString("path");
            partitions.add(new FilePartition(ipAddress, path));
        }
        return partitions;
    }

    // Gets location constraints for dataset scans.    
    private static String[] getScanLocationConstraints(List<FilePartition> filePartitions,
            Map<String, String> ipToNcNames) {
        String[] locations = new String[filePartitions.size()];
        for (int i = 0; i < locations.length; i++) {
            String ipAddress = filePartitions.get(i).getIPAddress();
            locations[i] = ipToNcNames.get(ipAddress);
        }
        return locations;
    }
}
