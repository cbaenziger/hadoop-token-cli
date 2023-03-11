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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hbase.security.User;

public class HbaseTokenProvider extends HadoopTokenProvider {

    @Override
    public boolean credentialsRequired(Configuration configuration) {
        return "kerberos".equals(configuration.get("hbase.security.authentication"));
    }

    @Override
    public Credentials obtainCredentials(Configuration conf) throws Exception, InterruptedException {
        Credentials creds = new Credentials();

        try(Connection conn = ConnectionFactory.createConnection(conf)) {
            User user = User.getCurrent();
            TokenUtil.addTokenIfMissing(conn, user);

            for (Token token : user.getTokens()) {
                if (token.getKind() == AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE) {
                    creds.addToken(token.getService(), token);
                }
            }
        }
        return creds;
    }

}
