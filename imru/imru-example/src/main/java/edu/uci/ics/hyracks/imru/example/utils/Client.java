/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.imru.example.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.imru.api.IIMRUDataGenerator;
import edu.uci.ics.hyracks.imru.api.IIMRUJob;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.IMRUJobControl;
import edu.uci.ics.hyracks.imru.data.DataSpreadDriver;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * This class wraps IMRU common functions.
 * Example usage: <blockquote>
 * 
 * <pre>
 * Client&lt;Model, MapReduceResult&gt; client = new Client&lt;Model, MapReduceResult&gt;(args);
 * //start a in-process cluster for debugging 
 * //client.startClusterAndNodes();  
 * client.connect();
 * client.uploadApp();
 * IMRUJob job = new IMRUJob();
 * JobStatus status = client.run(job);
 * Model finalModel = client.getModel();
 * </pre>
 * 
 * </blockquote>
 * 
 * @author wangrui
 * @param <Model>
 *            IMRU model which will be used in map() and updated in update()
 * @param <T>
 *            Object which is generated by map(), aggregated in reduce() and
 *            used in update()
 */
public class Client<Model extends Serializable, Data extends Serializable> {
    public static class Options {
        @Option(name = "-debug", usage = "Start cluster controller and node controller in this process for debugging")
        public boolean debug;

        @Option(name = "-debugNodes", usage = "Number of nodes started for debugging")
        public int numOfNodes = 2;

        @Option(name = "-disable-logging", usage = "Disable logging. So console output can be seen when debugging")
        public boolean disableLogging;

        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 3099;

        @Option(name = "-clusterport", usage = "Hyracks Cluster Controller Port (default: 3099)")
        public int clusterPort = 1099;

        @Option(name = "-imru-port", usage = "IMRU web service port for uploading and downloading models. (default: 3288)")
        public int imruPort = 3288;

        @Option(name = "-mem-cache", usage = "Load all data into memory")
        public boolean memCache = false;

        @Option(name = "-no-disk-cache", usage = "Don't cache data on local disk (only works with local data)")
        public boolean noDiskCache = false;

        @Option(name = "-app", usage = "Hyracks Application name")
        public String app = "imru-examples";

        @Option(name = "-hadoop-conf", usage = "Path to Hadoop configuration")
        public String hadoopConfPath;

        @Option(name = "-cluster-conf", usage = "Path to Hyracks cluster configuration")
        public String clusterConfPath = "conf/cluster.conf";

        @Option(name = "-cc-temp-path", usage = "Path on cluster controller to hold models")
        public String ccTempPath = "/tmp/imru-cc-models";

        @Option(name = "-save-intermediate-models", usage = "If specified, save intermediate models to this directory.")
        public String localIntermediateModelPath;

        @Option(name = "-model-file-name", usage = "Name of the model file")
        public String modelFileNameHDFS;

        @Option(name = "-input-paths", usage = "HDFS path to hold input data. Or local file in the format of [nodeId]:<path>")
        public String inputPaths;

        @Deprecated
        @Option(name = "-example-paths", usage = "Same as -input-paths")
        public String examplePaths = "/input/data.txt";

        @Option(name = "-agg-tree-type", usage = "The aggregation tree type (none, nary, or generic)")
        public String aggTreeType;

        @Option(name = "-agg-count", usage = "The number of aggregators to use, if using an aggregation tree")
        public int aggCount = -1;

        @Option(name = "-fan-in", usage = "The fan-in, if using an nary aggregation tree")
        public int fanIn = -1;

        @Option(name = "-model-file", usage = "Local file to write the final weights to")
        public String modelFilename;

        @Option(name = "-frame-size", usage = "Hyracks frame size")
        public int frameSize = 0;

        //        @Option(name = "-num-rounds", usage = "The number of iterations to perform")
        //        public int numRounds = 5;
    }

    public static String IMRU_PREFIX = "hyracks-auto-deploy-";
    public static int FRAME_SIZE = 65536;

    private static ClusterControllerService cc;
    private static Vector<NodeControllerService> ncs = new Vector<NodeControllerService>();
    protected HyracksConnection hcc;
    protected DeploymentId deploymentId;

    public IMRUJobControl<Model, Data> control;
    public Options options = new Options();
    Configuration conf;
    private static boolean alreadyStartedDebug = false;

    /**
     * Create a client object using a list of arguments
     * 
     * @param args
     * @throws CmdLineException
     */
    public Client(String[] args) throws CmdLineException {
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
    }

    /**
     * Return local host name
     * 
     * @return
     * @throws Exception
     */
    public static String getLocalHostName() throws Exception {
        return java.net.InetAddress.getLocalHost().getHostName();
    }

    /**
     * Return same ip as imru/imru-dist/target/appassembler/bin/getip.sh
     * 
     * @return
     * @throws Exception
     */
    public static String getLocalIp() throws Exception {
        String ip = "127.0.0.1";
        NetworkInterface netint = NetworkInterface.getByName("eth0");
        if (netint != null) {
            Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
            for (InetAddress inetAddress : Collections.list(inetAddresses)) {
                byte[] addr = inetAddress.getAddress();
                if (addr != null && addr.length == 4)
                    ip = inetAddress.getHostAddress();
            }
        }
        return ip;
    }

    /**
     * Generate cluster config file, required by IMRU
     * 
     * @param file
     *            the config file to be written
     * @param args
     *            a list of ip and node names
     * @throws IOException
     */
    public static void generateClusterConfig(File file, String... args)
            throws IOException {
        PrintStream ps = new PrintStream(file);
        for (int i = 0; i < args.length / 2; i++)
            ps.println(args[i * 2] + " " + args[i * 2 + 1]);
        ps.close();
    }

    /**
     * connect to the cluster controller
     * 
     * @throws Exception
     */
    public void connect() throws Exception {
        this.control = new IMRUJobControl<Model, Data>();
        control.localIntermediateModelPath = options.localIntermediateModelPath;
        control.modelFileName = options.modelFileNameHDFS;
        control.memCache = options.memCache;
        control.noDiskCache = options.noDiskCache;
        control.frameSize = options.frameSize;
        control.connect(options.host, options.port, options.imruPort,
                options.hadoopConfPath, options.clusterConfPath);
        hcc = control.hcc;
        conf = control.confFactory.createConfiguration();
        if (options.inputPaths != null)
            options.examplePaths = options.inputPaths;
        // set aggregation type
        if (options.aggTreeType == null) {
            int mappers = options.examplePaths.split(",").length;
            //            Map<String, NodeControllerInfo> map = hcc.getNodeControllerInfos();
            if (mappers < 3)
                control.selectNoAggregation(options.examplePaths);
            else
                control.selectNAryAggregation(options.examplePaths, 2);
        } else if (options.aggTreeType.equals("none")) {
            control.selectNoAggregation(options.examplePaths);
        } else if (options.aggTreeType.equals("generic")) {
            control.selectGenericAggregation(options.examplePaths,
                    options.aggCount);
        } else if (options.aggTreeType.equals("nary")) {
            Map<String, NodeControllerInfo> map = hcc.getNodeControllerInfos();
            if (map.size() < 3) {
                Rt.p("Change to generic aggregation because there are only "
                        + map.size() + " nodes");
                control.selectGenericAggregation(options.examplePaths,
                        options.fanIn);
            } else {
                control.selectNAryAggregation(options.examplePaths,
                        options.fanIn);
            }
        } else {
            throw new IllegalArgumentException("Invalid aggregation tree type");
        }
        // hyracks connection
    }

    public static boolean isServerAvailable(String host, int port)
            throws Exception {
        try {
            Socket socket = new Socket(host, port);
            socket.close();
            return true;
        } catch (java.net.ConnectException e) {
        }
        return false;
    }

    /**
     * run IMRU job
     * 
     * @throws Exception
     */
    public <T extends Serializable> JobStatus run(IIMRUJob<Model, Data, T> job,
            Model initialModel) throws Exception {
        return control.run(deploymentId, job, initialModel, options.app);
    }

    /**
     * run IMRU job using low level interface
     * 
     * @throws Exception
     */
    public JobStatus run(IIMRUJob2<Model, Data> job, Model initialModel)
            throws Exception {
        return control.run(deploymentId, job, initialModel, options.app);
    }

    public JobStatus generateData(IIMRUDataGenerator generator)
            throws Exception {
        return control.generateData(deploymentId, generator, options.app);
    }

    /**
     * @return a handle to HDFS
     * @throws IOException
     */
    public FileSystem getHDFS() throws IOException {
        return FileSystem.get(conf);
    }

    /**
     * start local cluster controller and two node controller for debugging
     * purpose
     * 
     * @throws Exception
     */
    public void startClusterAndNodes() throws Exception {
        startCC(options.host, options.clusterPort, options.port);
        for (int i = 0; i < options.numOfNodes; i++)
            startNC("NC" + i, options.host, options.clusterPort);
    }

    /**
     * Start a cluster controller
     * 
     * @param host
     * @param clusterNetPort
     * @param clientNetPort
     * @throws Exception
     */
    public void startCC(String host, int clusterNetPort, int clientNetPort)
            throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = host;
        ccConfig.clusterNetIpAddress = host;
        ccConfig.clusterNetPort = clusterNetPort;
        ccConfig.clientNetPort = clientNetPort;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.jobHistorySize = 10;
        ccConfig.appCCMainClass = "edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUCCBootstrapImpl";

        // cluster controller
        cc = new ClusterControllerService(ccConfig);
        cc.start();
    }

    /**
     * Start the first node controller
     * 
     * @param NC1_ID
     * @param host
     * @param clusterNetPort
     * @throws Exception
     */
    public void startNC(String NC1_ID, String host, int clusterNetPort)
            throws Exception {
        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = host;
        ncConfig1.clusterNetIPAddress = host;
        ncConfig1.ccPort = clusterNetPort;
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.resultIPAddress = "127.0.0.1";
        ncConfig1.appNCMainClass = "edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUNCBootstrapImpl";
        ncConfig1.nodeId = NC1_ID;
        File file = new File("/tmp/cache/tmp/" + NC1_ID);
        file.mkdirs();
        ncConfig1.ioDevices = file.getAbsolutePath();
        NodeControllerService nc = new NodeControllerService(ncConfig1);
        nc.start();
        ncs.add(nc);
    }

    /**
     * disable logs
     * 
     * @throws Exception
     */
    public static void disableLogging() throws Exception {
        Logger globalLogger = Logger.getLogger("");
        Handler[] handlers = globalLogger.getHandlers();
        for (Handler handler : handlers)
            globalLogger.removeHandler(handler);
        globalLogger.addHandler(new Handler() {
            @Override
            public void publish(LogRecord record) {
                String s = record.getMessage();
                if (s.contains("Exception caught by thread")) {
                    System.err.println(s);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() throws SecurityException {
            }
        });
    }

    /**
     * Remove the application
     * 
     * @param hyracksAppName
     * @throws Exception
     */
    public void destroyApp(DeploymentId deploymentId) throws Exception {
        hcc.unDeployBinary(deploymentId);
    }

    /**
     * Stop cluster controller and node controllers
     * 
     * @throws Exception
     */
    public static void stop() throws Exception {
        for (NodeControllerService nc : ncs)
            nc.stop();
        ncs.clear();
        cc.stop();
    }

    /**
     * Run an already uploaded job
     * 
     * @param spec
     * @param appName
     * @throws Exception
     */
    public void runJob(JobSpecification spec, String appName) throws Exception {
        spec.setFrameSize(FRAME_SIZE);
        JobId jobId = hcc.startJob(deploymentId, spec, EnumSet
                .of(JobFlag.PROFILE_RUNTIME));
        hcc.waitForCompletion(jobId);
    }

    /**
     * Create a HAR file contains jars specified in .classpath and uploaded to
     * hyracks cluster
     * 
     * @throws Exception
     */
    public DeploymentId uploadApp() throws Exception {
        return uploadApp(hcc, options.app, options.hadoopConfPath != null,
                options.imruPort, options.ccTempPath, options.debug);
    }

    public static DeploymentId uploadApp(HyracksConnection hcc, String appName,
            boolean includeHadoop, int imruPort, String tempDir)
            throws Exception {
        return uploadApp(hcc, appName, includeHadoop, imruPort, tempDir, false);
    }

    public static DeploymentId uploadApp(HyracksConnection hcc, String appName,
            boolean includeHadoop, int imruPort, String tempDir, boolean debug)
            throws Exception {
        DeploymentId deploymentId = null;
        Timer timer = new Timer();
        //        File harFile = null;
        Vector<String> jars = new Vector<String>();
        Vector<String> tmpjars = new Vector<String>();
        final long length = CreateHar.createJars(includeHadoop, imruPort,
                tempDir, jars, tmpjars);
        if (!debug) {
            //            harFile = File.createTempFile("imru_app", ".zip");
            //            FileOutputStream out = new FileOutputStream(harFile);
            //            CreateHar.createHar(harFile, includeHadoop, imruPort, tempDir);
            //            out.close();
            //            final File harFile2 = harFile;
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    Rt.p("Uploading jar files (%.2f MB). "
                            + "If there isn't any response for a long time, "
                            + "please check nc logs, "
                            + "there might be ClassNotFoundException.",
                            length / 1024.0 / 1024.0);
                }
            }, 2000);
        }
        try {
            for (String s : jars)
                System.out.println("uploading jar: " + s);
            deploymentId = hcc.deployBinary(jars);
            //            hcc.createApplication(appName, harFile);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                Rt.p("Remove application " + appName);
                hcc.unDeployBinary(deploymentId);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            Rt.p("Upload application " + appName);
            hcc.deployBinary(jars);
        }
        timer.cancel();
        for (String s : tmpjars) {
            Rt.p("remove jar " + s);
            new File(s).delete();
        }
        return deploymentId;
    }

    /**
     * @return The most recent model.
     */
    public Model getModel() {
        return control.getModel();
    }

    /**
     * start local cluster controller and two node controller for debugging
     * purpose
     * 
     * @throws Exception
     */
    public static void startClusterAndNodes(String[] args) throws Exception {
        Client clent = new Client<Serializable, Serializable>(args);
        clent.startClusterAndNodes();
    }

    public void init() throws Exception {
        // disable logs
        if (options.disableLogging)
            Client.disableLogging();

        // start local cluster controller and two node controller
        // for debugging purpose
        if (options.debug && !alreadyStartedDebug) {
            alreadyStartedDebug = true;
            startClusterAndNodes();
        }

        // connect to the cluster controller
        connect();

        // create the application in local cluster
        deploymentId = uploadApp();
    }

    /**
     * run job
     * 
     * @throws Exception
     */
    public static <M extends Serializable, D extends Serializable, R extends Serializable> M run(
            IIMRUJob<M, D, R> job, M initialModel, String[] args)
            throws Exception {
        return run(job, initialModel, args, null);
    }

    /**
     * run job
     * 
     * @throws Exception
     */
    public static <M extends Serializable, D extends Serializable, R extends Serializable> M run(
            IIMRUJob<M, D, R> job, M initialModel, String[] args,
            String overrideAppName) throws Exception {
        // create a client object, which handles everything
        Client<M, D> client = new Client<M, D>(args);
        try {

            if (overrideAppName != null)
                client.options.app = overrideAppName;
            client.init();

            // run job
            JobStatus status = client.run(job, initialModel);
            if (status == JobStatus.FAILURE) {
                System.err.println("Job failed; see CC and NC logs");
                System.exit(-1);
            }
            // System.out.println("Terminated after "
            // + client.control.getIterationCount() + " iterations");

            return client.getModel();
        } finally {
            //            if (client.options.debug)
            //                System.exit(0);
        }
    }

    public static <M extends Serializable, D extends Serializable, R extends Serializable> M generateData(
            IIMRUDataGenerator generator, String[] args) throws Exception {
        // create a client object, which handles everything
        Client<M, D> client = new Client<M, D>(args);
        try {
            client.init();
            // run job
            JobStatus status = client.generateData(generator);
            if (status == JobStatus.FAILURE) {
                System.err.println("Job failed; see CC and NC logs");
                System.exit(-1);
            }
            // System.out.println("Terminated after "
            // + client.control.getIterationCount() + " iterations");

            return client.getModel();
        } finally {
            //            if (client.options.debug)
            //                System.exit(0);
        }
    }

    /**
     * Spread data into specified nodes in $log_2 N$ time.
     * 
     * @throws Exception
     */
    public static void distributeData(DeploymentId deploymentId, File[] src,
            String[] targetNodes, String[] dest, String[] args)
            throws Exception {
        // create a client object, which handles everything
        Client<Serializable, Serializable> client = new Client<Serializable, Serializable>(
                args);

        client.init();

        for (int i = 0; i < src.length; i++) {
            DataSpreadDriver driver = new DataSpreadDriver(client.hcc,
                    deploymentId, client.control.imruConnection,
                    client.options.app, src[i], targetNodes, dest[i]);
            JobStatus status = driver.run();
            if (status == JobStatus.FAILURE) {
                System.err.println("Job failed; see CC and NC logs");
                System.exit(-1);
            }
        }
    }
}
