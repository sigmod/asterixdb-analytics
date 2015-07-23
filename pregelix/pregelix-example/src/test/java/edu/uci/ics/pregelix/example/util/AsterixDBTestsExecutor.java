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

import org.apache.hadoop.fs.Path;

import edu.uci.ics.asterix.test.aql.TestsExecutor;
import edu.uci.ics.asterix.testframework.context.TestFileContext;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.base.IDriver.Plan;
import edu.uci.ics.pregelix.core.driver.Driver;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;

public class AsterixDBTestsExecutor extends TestsExecutor {

    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";

    // Executes external runtimes.
    @Override
    protected void executeExternalRuntime(TestFileContext ctx) throws Exception {
        String jobFile = ctx.getFile().getAbsolutePath();
        PregelixJob job = new PregelixJob("test");
        job.getConfiguration().addResource(new Path(jobFile));
        job.getConfiguration().addResource(new Path(HADOOP_CONF_PATH));

        System.out.println("TEST: " + jobFile);
        Driver driver = new Driver(AsterixDBTestsExecutor.class);
        Plan[] plans = new Plan[] { Plan.OUTER_JOIN };
        for (Plan plan : plans) {
            job.setMergeConnector(false);
            driver.runJob(job, plan, PregelixHyracksIntegrationUtil.CC_HOST,
                    PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT, false);
        }
    }

}
