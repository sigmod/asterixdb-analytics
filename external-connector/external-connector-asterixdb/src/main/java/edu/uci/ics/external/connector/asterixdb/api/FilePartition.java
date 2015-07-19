package edu.uci.ics.external.connector.asterixdb.api;

import org.json.JSONException;
import org.json.JSONObject;

public class FilePartition {
    private final String ipAddress;
    private final String path;
    private final int ioDeviceId;

    public FilePartition(String ipAddress, String path, int ioDeviceId) {
        this.ipAddress = ipAddress;
        this.path = path;
        this.ioDeviceId = ioDeviceId;
    }

    public String getIPAddress() {
        return ipAddress;
    }

    public String getPath() {
        return path;
    }

    public int getIODeviceId() {
        return ioDeviceId;
    }

    @Override
    public String toString() {
        return ipAddress + ":" + path + ":" + ioDeviceId;
    }

    public JSONObject toJSONObject() throws JSONException {
        JSONObject partition = new JSONObject();
        partition.put("ip", ipAddress);
        partition.put("path", path);
        partition.put("ioDeviceId", ioDeviceId);
        return partition;
    }
}
