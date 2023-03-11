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

package com.bloomberg.hi.dtclient.provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.bloomberg.hi.dtclient.model.HadoopServiceTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.security.DelegationTokenIdentifier;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class HiveTokenProvider extends HadoopTokenProvider {
    private Logger log = LoggerFactory.getLogger(HiveTokenProvider.class);

    @Override
    public boolean credentialsRequired(Configuration hiveConfiguration) {
        return UserGroupInformation.isSecurityEnabled() && hiveConfiguration.getTrimmed("hive.metastore.uris", "").length() > 0 ;
    }

    public Credentials obtainCredentials(Configuration hiveConfiguration) throws Exception, InterruptedException {
        String base64Str = null;
        Credentials creds = new Credentials();
        String principalKey = "hive.metastore.kerberos.principal";

        String principal = hiveConfiguration.getTrimmed(principalKey, "");

        creds.addToken(new Text("hive.server2.delegation.token"), doAsRealUser(hiveConfiguration, principal));
        return creds;
    }

    private Token<?> doAsRealUser(final Configuration hiveConfiguration, final String principal ) throws IOException, InterruptedException {
        final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        UserGroupInformation realUser = currentUser.getRealUser() != null ? currentUser.getRealUser() : currentUser;
        return realUser.doAs(new PrivilegedExceptionAction<Token<?>>(){
            public Token<?> run() throws Exception {
                Token<DelegationTokenIdentifier> identifier = new Token<DelegationTokenIdentifier>();
                try {
                    HiveMetaStoreClient hive = new HiveMetaStoreClient(new HiveConf(hiveConfiguration, HiveConf.class));
                    String tokenStr =  hive.getDelegationToken(currentUser.getUserName(), principal);
                    identifier.decodeFromUrlString(tokenStr);
                } catch (Exception e) {
                    String message = String.format("Unable to obtain token for %s due to %s", HadoopServiceTypes.HIVE, e.getMessage());
                    log.error(message, e);
                    throw new Exception(message);
                }

                return  identifier;
            }
        });
    }
}
