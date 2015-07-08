package edu.uci.ics.external.connector.asterixdb.api;

import org.json.JSONException;
import org.json.JSONObject;

public class FilePartition {
    private final String ipAddress;
    private final String path;

    public FilePartition(String ipAddress, String path) {
        this.ipAddress = ipAddress;
        this.path = path;
    }

    public String getIPAddress() {
        return ipAddress;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return ipAddress + ":" + path;
    }

    public JSONObject toJSONObject() throws JSONException {
        JSONObject partition = new JSONObject();
        partition.put("ip", ipAddress);
        partition.put("path", path);
        return partition;
    }
}
