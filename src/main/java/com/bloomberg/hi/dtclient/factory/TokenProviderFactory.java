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

package com.bloomberg.hi.dtclient.factory;

import com.bloomberg.hi.dtclient.model.HadoopServiceTypes;
import com.bloomberg.hi.dtclient.provider.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
public class TokenProviderFactory {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private long defaultTicketLifetime;


    public long getDefaultTicketLifetime() {
        return defaultTicketLifetime;
    }

    public void setDefaultTicketLifetime(long defaultTicketLifetime) {
        this.defaultTicketLifetime = defaultTicketLifetime;
    }

    // privates
    public Credentials fetchCredentials (HadoopServiceTypes service, String proxyUser, UserGroupInformation userGroupInformation, final Configuration configuration) throws Exception {
        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUser(proxyUser, userGroupInformation);

        Credentials credentials = proxyUgi.doAs(
                new PrivilegedExceptionAction<Credentials>() {
            public Credentials run() throws Exception {
                Credentials tmpCreds = null;
                HadoopTokenProvider serviceTokenProvider = null;
                switch (service) {
                    case HBASE:
                        serviceTokenProvider = new HbaseTokenProvider();
                        break;
                    case HIVE:
                        serviceTokenProvider = new HiveTokenProvider();
                        break;
                    case HDFS:
                        serviceTokenProvider = new HdfsTokenProvider();
                        break;
                    case WEBHDFS:
                        serviceTokenProvider = new WebHdfsTokenProvider();
                        //Check for webHdfsTokenProvider.credentialsRequired()
                        break;
                    default:
                        log.error("Unknown service type found %s");
                        throw new Exception("Unknown Service type found");
                }

                try {
                    tmpCreds = serviceTokenProvider.obtainCredentials(configuration);
                } catch (Exception e) {
                    String message = String.format("Unable to obtain token for %s due to %s", service.name(), e.getMessage());
                    log.error(message, e);
                    throw new Exception(message);
                }
                return tmpCreds;
            }
        });
        return credentials;
    }
}
