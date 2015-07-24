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

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.util.JSONDeserializerForTypes;
import edu.uci.ics.external.connector.asterixdb.api.FilePartition;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;

public class ConnectorUtils {

    // Retrieves the type and partition information of the target AsterixDB dataset.
    public static DatasetInfo retrieveDatasetInfo(StorageParameter storageParameter) throws Exception {
        HttpClient client = new HttpClient();
        String requestStr = storageParameter.getServiceURL() + "/connector" + "?dataverseName="
                + storageParameter.getDataverseName() + "&datasetName=" + storageParameter.getDatasetName();
        // Create a method instance.
        GetMethod method = new GetMethod(requestStr);

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        // Deals with the response.
        // Executes the method.
        try {
            int statusCode = client.executeMethod(method);
            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Method failed: " + method.getStatusLine());
            }

            // Deals with the response.
            JSONTokener tokener = new JSONTokener(new InputStreamReader(method.getResponseBodyAsStream()));
            JSONObject response = new JSONObject(tokener);

            // Checks if there are errors.
            String error = "error";
            if (response.has(error)) {
                throw new IllegalStateException("AsterixDB returned errors: " + response.getString(error));
            }

            // Extracts record type and file partitions.
            boolean temp = extractTempInfo(response);
            ARecordType recordType = extractRecordType(response);
            List<FilePartition> filePartitions = extractFilePartitions(response);
            IFileSplitProvider fileSplitProvider = createFileSplitProvider(storageParameter, filePartitions);
            String[] locations = getScanLocationConstraints(filePartitions, storageParameter.getIpToNcNames());
            String[] primaryKeys = response.getString("keys").split(",");
            DatasetInfo datasetInfo = new DatasetInfo(locations, fileSplitProvider, recordType, primaryKeys, temp);
            return datasetInfo;
        } catch (Exception e) {
            throw e;
        } finally {
            method.releaseConnection();
        }
    }

    public static void cleanDataset(StorageParameter storageParameter, DatasetInfo datasetInfo) throws Exception {
        // DDL service URL.
        String url = storageParameter.getServiceURL() + "/ddl";
        // Builds the DDL string to delete and (re-)create the sink datset.
        StringBuilder ddlBuilder = new StringBuilder();
        // use dataverse statement.
        ddlBuilder.append("use dataverse ");
        ddlBuilder.append(storageParameter.getDataverseName());
        ddlBuilder.append(";");

        // drop dataset statement.
        ddlBuilder.append("drop dataset ");
        ddlBuilder.append(storageParameter.getDatasetName());
        ddlBuilder.append(";");

        // create datset statement.
        ddlBuilder.append("create temporary dataset ");
        ddlBuilder.append(storageParameter.getDatasetName());
        ddlBuilder.append("(");
        ddlBuilder.append(datasetInfo.getRecordType().getTypeName());
        ddlBuilder.append(")");
        ddlBuilder.append(" primary key ");
        for (String primaryKey : datasetInfo.getPrimaryKeyFields()) {
            ddlBuilder.append(primaryKey);
            ddlBuilder.append(",");
        }
        ddlBuilder.delete(ddlBuilder.length() - 1, ddlBuilder.length());
        ddlBuilder.append(";");

        // Create a method instance.
        PostMethod method = new PostMethod(url);
        method.setRequestEntity(new StringRequestEntity(ddlBuilder.toString()));
        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        // Execute the method.
        executeHttpMethod(method);
    }

    // Creates file split provider for BTree operators.
    private static IFileSplitProvider createFileSplitProvider(StorageParameter storageParameter,
            List<FilePartition> inputs) {
        FileSplit[] splits = new FileSplit[inputs.size()];
        int i = 0;
        for (FilePartition p : inputs) {
            // make sure the path has a slash at the end
            String path = p.getPath();
            if (!path.endsWith("/")) {
                path += "/";
            }
            List<String> ncNames = storageParameter.getIpToNcNames().get(p.getIPAddress());
            String ncName = ncNames.get(0);
            splits[i++] = new FileSplit(ncName, path, p.getIODeviceId());
        }
        return new ConstantFileSplitProvider(splits);
    }

    // Extracts the info indicating whether the dataset is temp or not
    private static boolean extractTempInfo(JSONObject response) throws Exception {
        return response.has("temp") ? response.getBoolean("temp") : false;
    }

    // Extracts the record type of a dataset from the AsterixDB REST response.
    private static ARecordType extractRecordType(JSONObject response) throws Exception {
        return (ARecordType) JSONDeserializerForTypes.convertFromJSON((JSONObject) response.get("type"));
    }

    // Extracts the file partitions of a dataset from the AsterixDB REST response.
    private static List<FilePartition> extractFilePartitions(JSONObject response) throws JSONException {
        // Converts to FilePartition arrays.
        List<FilePartition> partitions = new ArrayList<FilePartition>();
        JSONArray splits = response.getJSONArray("splits");
        for (int i = 0; i < splits.length(); i++) {
            String ipAddress = ((JSONObject) splits.get(i)).getString("ip");
            String path = ((JSONObject) splits.get(i)).getString("path");
            int ioDeviceId = ((JSONObject) splits.get(i)).getInt("ioDeviceId");
            partitions.add(new FilePartition(ipAddress, path, ioDeviceId));
        }
        return partitions;
    }

    // Gets location constraints for dataset scans.
    private static String[] getScanLocationConstraints(List<FilePartition> filePartitions,
            Map<String, List<String>> ipToNcNames) {
        String[] locations = new String[filePartitions.size()];
        for (int i = 0; i < locations.length; i++) {
            String ipAddress = filePartitions.get(i).getIPAddress();
            List<String> ncNames = ipToNcNames.get(ipAddress);
            locations[i] = ncNames.get(0);
        }
        return locations;
    }

    // Executes a HTTP method.
    private static int executeHttpMethod(HttpMethod method) throws Exception {
        HttpClient client = new HttpClient();
        int statusCode;
        try {
            statusCode = client.executeMethod(method);
            if (statusCode != HttpStatus.SC_OK) {
                JSONObject result = new JSONObject(new JSONTokener(new InputStreamReader(
                        method.getResponseBodyAsStream())));
                if (result.has("error-code")) {
                    String[] errors = { result.getJSONArray("error-code").getString(0), result.getString("summary"),
                            result.getString("stacktrace") };
                    throw new Exception("HTTP operation failed: " + errors[0] + "\nSTATUS LINE: "
                            + method.getStatusLine() + "\nSUMMARY: " + errors[1] + "\nSTACKTRACE: " + errors[2]);
                }
            }
            return statusCode;
        } catch (Exception e) {
            throw e;
        } finally {
            method.releaseConnection();
        }
    }
}
