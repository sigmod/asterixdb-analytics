package edu.uci.ics.pregelix.example.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;

import edu.uci.ics.asterix.testframework.context.TestCaseContext.OutputFormat;

public class RESTRunner {

    @Test
    public void test() throws Exception {
        File directory = new File("queries");
        String[] fileNames = directory.list();
        Arrays.sort(fileNames);
        for (String fileName : fileNames) {
            String statement = readTestFile(new File(directory.getName() + File.separator + fileName));
            long start = System.currentTimeMillis();
            if (!(fileName.startsWith("1") || fileName.startsWith("3"))) {
                continue;
            }
            System.out.println(fileName);
            if (fileName.endsWith("query")) {
                executeQuery(statement, OutputFormat.ADM);
            } else if (fileName.endsWith("update")) {
                executeUpdate(statement);
            } else if (fileName.endsWith("ddl")) {
                executeDDL(statement);
            }
            long end = System.currentTimeMillis();
            System.out.println("Execution Time: " + (end - start) + " milliseconds");
            System.out.println();
        }
    }

    // To execute DDL and Update statements
    // create type statement
    // create dataset statement
    // create index statement
    // create dataverse statement
    // create function statement
    public void executeDDL(String str) throws Exception {
        final String url = "http://sensorium-1.ics.uci.edu:18002/ddl";

        // Create a method instance.
        PostMethod method = new PostMethod(url);
        method.setRequestEntity(new StringRequestEntity(str));
        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        // Execute the method.
        executeHttpMethod(method);
    }

    // Executes Query and returns results as JSONArray
    public InputStream executeQuery(String str, OutputFormat fmt) throws Exception {
        final String url = "http://sensorium-1.ics.uci.edu:18002/query";

        // Create a method instance.
        GetMethod method = new GetMethod(url);
        method.setQueryString(new NameValuePair[] { new NameValuePair("query", str) });
        method.setRequestHeader("Accept", fmt.mimeType());

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
        executeHttpMethod(method);
        return method.getResponseBodyAsStream();
    }

    // To execute Update statements
    // Insert and Delete statements are executed here
    public void executeUpdate(String str) throws Exception {
        final String url = "http://sensorium-1.ics.uci.edu:18002/update";

        // Create a method instance.
        PostMethod method = new PostMethod(url);
        method.setRequestEntity(new StringRequestEntity(str));

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        // Execute the method.
        executeHttpMethod(method);
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

    // Method that reads a DDL/Update/Query File
    // and returns the contents as a string
    // This string is later passed to REST API for execution.
    private static String readTestFile(File testFile) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(testFile));
        String line = null;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");

        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }
        reader.close();
        return stringBuilder.toString();
    }
}
