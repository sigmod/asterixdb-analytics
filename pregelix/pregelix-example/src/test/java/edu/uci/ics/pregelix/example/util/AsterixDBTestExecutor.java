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

package edu.uci.ics.pregelix.example.util;

import java.io.File;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.List;

import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.context.TestCaseContext.OutputFormat;
import org.apache.asterix.testframework.context.TestFileContext;
import org.apache.asterix.testframework.xml.TestCase.CompilationUnit;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.base.IDriver.Plan;
import edu.uci.ics.pregelix.core.driver.Driver;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;

public class AsterixDBTestExecutor extends TestExecutor {

    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";

    @Override
    public void executeTest(String actualPath, TestCaseContext testCaseCtx, ProcessBuilder pb,
            boolean isDmlRecoveryTest, TestGroup failedGroup) throws Exception {

        File testFile;
        File expectedResultFile;
        String statement;
        List<TestFileContext> expectedResultFileCtxs;
        List<TestFileContext> testFileCtxs;
        File qbcFile = null;
        File qarFile = null;
        int queryCount = 0;

        List<CompilationUnit> cUnits = testCaseCtx.getTestCase().getCompilationUnit();
        for (CompilationUnit cUnit : cUnits) {
            LOGGER.info(
                    "Starting [TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName() + " ... ");
            testFileCtxs = testCaseCtx.getTestFiles(cUnit);
            expectedResultFileCtxs = testCaseCtx.getExpectedResultFiles(cUnit);
            for (TestFileContext ctx : testFileCtxs) {
                testFile = ctx.getFile();
                statement = readTestFile(testFile);
                boolean failed = false;
                try {
                    switch (ctx.getType()) {
                        case "ddl":
                            if (ctx.getFile().getName().endsWith("aql")) {
                                executeDDL(statement, "http://localhost:19002/ddl");
                            } else {
                                executeDDL(statement, "http://localhost:19002/ddl/sqlpp");
                            }
                            break;
                        case "update":
                            //isDmlRecoveryTest: set IP address
                            if (isDmlRecoveryTest && statement.contains("nc1://")) {
                                statement = statement.replaceAll("nc1://",
                                        "127.0.0.1://../../../../../../asterix-app/");
                            }
                            if (ctx.getFile().getName().endsWith("aql")) {
                                executeUpdate(statement, "http://localhost:19002/update");
                            } else {
                                executeUpdate(statement, "http://localhost:19002/update/sqlpp");
                            }
                            break;
                        case "query":
                        case "async":
                        case "asyncdefer":
                            // isDmlRecoveryTest: insert Crash and Recovery
                            if (isDmlRecoveryTest) {
                                executeScript(pb, pb.environment().get("SCRIPT_HOME") + File.separator + "dml_recovery"
                                        + File.separator + "kill_cc_and_nc.sh");
                                executeScript(pb, pb.environment().get("SCRIPT_HOME") + File.separator + "dml_recovery"
                                        + File.separator + "stop_and_start.sh");
                            }
                            InputStream resultStream = null;
                            OutputFormat fmt = OutputFormat.forCompilationUnit(cUnit);
                            if (ctx.getFile().getName().endsWith("aql")) {
                                if (ctx.getType().equalsIgnoreCase("query")) {
                                    resultStream = executeQuery(statement, fmt, "http://localhost:19002/query",
                                            cUnit.getParameter());
                                } else if (ctx.getType().equalsIgnoreCase("async")) {
                                    resultStream = executeAnyAQLAsync(statement, false, fmt,
                                            "http://localhost:19002/aql");
                                } else if (ctx.getType().equalsIgnoreCase("asyncdefer")) {
                                    resultStream = executeAnyAQLAsync(statement, true, fmt,
                                            "http://localhost:19002/aql");
                                }
                            } else {
                                if (ctx.getType().equalsIgnoreCase("query")) {
                                    resultStream = executeQuery(statement, fmt, "http://localhost:19002/query/sqlpp",
                                            cUnit.getParameter());
                                } else if (ctx.getType().equalsIgnoreCase("async")) {
                                    resultStream = executeAnyAQLAsync(statement, false, fmt,
                                            "http://localhost:19002/sqlpp");
                                } else if (ctx.getType().equalsIgnoreCase("asyncdefer")) {
                                    resultStream = executeAnyAQLAsync(statement, true, fmt,
                                            "http://localhost:19002/sqlpp");
                                }
                            }

                            if (queryCount >= expectedResultFileCtxs.size()) {
                                throw new IllegalStateException(
                                        "no result file for " + testFile.toString() + "; queryCount: " + queryCount
                                                + ", filectxs.size: " + expectedResultFileCtxs.size());
                            }
                            expectedResultFile = expectedResultFileCtxs.get(queryCount).getFile();

                            File actualResultFile = testCaseCtx.getActualResultFile(cUnit, new File(actualPath));
                            actualResultFile.getParentFile().mkdirs();
                            writeOutputToFile(actualResultFile, resultStream);

                            runScriptAndCompareWithResult(testFile, new PrintWriter(System.err), expectedResultFile,
                                    actualResultFile);
                            LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName()
                                    + " PASSED ");

                            queryCount++;
                            break;
                        case "mgx":
                            executeManagixCommand(statement);
                            break;
                        case "txnqbc": //qbc represents query before crash
                            resultStream = executeQuery(statement, OutputFormat.forCompilationUnit(cUnit),
                                    "http://localhost:19002/query", cUnit.getParameter());
                            qbcFile = new File(actualPath + File.separator
                                    + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                                    + cUnit.getName() + "_qbc.adm");
                            qbcFile.getParentFile().mkdirs();
                            writeOutputToFile(qbcFile, resultStream);
                            break;
                        case "txnqar": //qar represents query after recovery
                            resultStream = executeQuery(statement, OutputFormat.forCompilationUnit(cUnit),
                                    "http://localhost:19002/query", cUnit.getParameter());
                            qarFile = new File(actualPath + File.separator
                                    + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                                    + cUnit.getName() + "_qar.adm");
                            qarFile.getParentFile().mkdirs();
                            writeOutputToFile(qarFile, resultStream);
                            runScriptAndCompareWithResult(testFile, new PrintWriter(System.err), qbcFile, qarFile);

                            LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName()
                                    + " PASSED ");
                            break;
                        case "txneu": //eu represents erroneous update
                            try {
                                executeUpdate(statement, "http://localhost:19002/update");
                            } catch (Exception e) {
                                //An exception is expected.
                                failed = true;
                                e.printStackTrace();
                            }
                            if (!failed) {
                                throw new Exception(
                                        "Test \"" + testFile + "\" FAILED!\n  An exception" + "is expected.");
                            }
                            System.err.println("...but that was expected.");
                            break;
                        case "sleep":
                            Thread.sleep(Long.parseLong(statement.trim()));
                            break;
                        case "errddl": // a ddlquery that expects error
                            try {
                                executeDDL(statement, "http://localhost:19002/ddl");
                            } catch (Exception e) {
                                // expected error happens
                                failed = true;
                                e.printStackTrace();
                            }
                            if (!failed) {
                                throw new Exception(
                                        "Test \"" + testFile + "\" FAILED!\n  An exception" + "is expected.");
                            }
                            System.err.println("...but that was expected.");
                            break;
                        default:
                            executeExternalRuntime(ctx);
                    }
                } catch (Exception e) {
                    System.err.println("testFile " + testFile.toString() + " raised an exception:");
                    e.printStackTrace();
                    if (cUnit.getExpectedError().isEmpty()) {
                        System.err.println("...Unexpected!");
                        if (failedGroup != null) {
                            failedGroup.getTestCase().add(testCaseCtx.getTestCase());
                        }
                        throw new Exception("Test \"" + testFile + "\" FAILED!", e);
                    } else {
                        LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName()
                                + " failed as expected: " + e.getMessage());
                        System.err.println("...but that was expected.");
                    }

                }
            }
        }
    }

    // Executes external runtimes.
    protected void executeExternalRuntime(TestFileContext ctx) throws Exception {
        String jobFile = ctx.getFile().getAbsolutePath();
        PregelixJob job = new PregelixJob("test");
        job.getConfiguration().addResource(new Path(jobFile));
        job.getConfiguration().addResource(new Path(HADOOP_CONF_PATH));

        System.out.println("TEST: " + jobFile);
        Driver driver = new Driver(AsterixDBTestExecutor.class);
        Plan[] plans = new Plan[] { Plan.OUTER_JOIN };
        for (Plan plan : plans) {
            job.setMergeConnector(false);
            driver.runJob(job, plan, PregelixHyracksIntegrationUtil.CC_HOST,
                    PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT, false);
        }
    }

    // For tests where you simply want the byte-for-byte output.
    private static void writeOutputToFile(File actualFile, InputStream resultStream) throws Exception {
        byte[] buffer = new byte[10240];
        int len;
        java.io.FileOutputStream out = new java.io.FileOutputStream(actualFile);
        try {
            while ((len = resultStream.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
        } finally {
            out.close();
        }
    }

}
