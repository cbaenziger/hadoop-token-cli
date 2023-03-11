/*
 ** Copyright 2023 Bloomberg Finance L.P.
 **
 ** Licensed under the Apache License, Version 2.0 (the "License");
 ** you may not use this file except in compliance with the License.
 ** You may obtain a copy of the License at
 **
 **     http://www.apache.org/licenses/LICENSE-2.0
 **
 ** Unless required by applicable law or agreed to in writing, software
 ** distributed under the License is distributed on an "AS IS" BASIS,
 ** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 ** See the License for the specific language governing permissions and
 ** limitations under the License.
 */

package com.bloomberg.hi.dtclient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

import com.bloomberg.hi.dtclient.managers.HadoopTokenRequestManager;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class DTGatherer extends Configured implements Tool {
    final static String NAME = "dtgatherer";
    protected static final Logger LOG = LoggerFactory.getLogger(DTGatherer.class);

    @Override
    public int run(String[] args) throws Exception {
      String keytabPrincipal = args[0];
      String keytabFile = args[1];
      String tokenFile = args[2];
      String requestedUser = args[3];
      if (args.length < 5) {
        System.err.println("Usage: dtgatherer <keytab principal> <keytab file> <token file> <requested user> <service1> [<service2>, ...]");
        System.exit(2);
      }
      LOG.info("Using principal: {}, keytab: {}, for user {}, writing file {}", keytabPrincipal, keytabFile, requestedUser, tokenFile);

      Configuration conf = new Configuration();
      HashMap<String, String> confDirs = new HashMap();
      confDirs.put("hadoop", System.getenv("HADOOP_CONF_DIR"));
      confDirs.put("hbase", System.getenv("HBASE_CONF_DIR"));
      confDirs.put("hive", System.getenv("HIVE_CONF_DIR"));
      for (Map.Entry<String, String> entry : confDirs.entrySet()){
	if(entry.getValue() != null) {
	  //  will add non-existant $HADOOP_CONF_DIR/hadoop-site.xml which is unchecked
	  //  just ensure the env. var. is set
          conf.addResource(new Path(entry.getValue(), entry.getKey() + "-site.xml"));
	} else {
          LOG.error("No path set for ${}_CONF_DIR", entry.getKey().toUpperCase());
          System.exit(2);
	}
      }
      conf.addResource(new Path(confDirs.get("hadoop"), "core-site.xml"));
      conf.addResource(new Path(confDirs.get("hadoop"), "hdfs-site.xml"));
      UserGroupInformation.setConfiguration(conf);

      HadoopTokenRequestManager hdtm = new HadoopTokenRequestManager(keytabPrincipal, keytabFile);
      Credentials creds = new Credentials();
      try {
        for (String service : Arrays.copyOfRange(args, 4, args.length)){
          LOG.info("Requesting service {}", service);
          creds.mergeAll(hdtm.fetchHadoopToken(requestedUser, conf, service));
        }
	DataOutputStream out = new DataOutputStream(new FileOutputStream(tokenFile));
	creds.writeTokenStorageToStream(out);
	out.close();
      } catch (Exception e) {
        LOG.error(String.format("Failed to fetch token."), e);
        throw(e);
      }
      return(0);
    }

    public static void main(String[] args) throws Exception {
      // Work around using the Maven assembly plugin and LOG4J2-673
      System.setProperty("log4j2.loggerContextFactory", "org.apache.logging.log4j.core.impl.Log4jContextFactory");
      int status = ToolRunner.run(new Configuration(), new DTGatherer(), args);
      System.exit(status);
    }
}
