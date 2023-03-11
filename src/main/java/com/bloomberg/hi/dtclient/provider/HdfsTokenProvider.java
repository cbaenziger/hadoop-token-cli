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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HdfsTokenProvider extends HadoopTokenProvider {
    private final Logger LOG = LoggerFactory.getLogger(HdfsTokenProvider.class);

    @Override
    public Credentials obtainCredentials(Configuration hadoopConf) throws Exception, InterruptedException {
        String base64Str = null;
        Credentials creds = new Credentials();
        Path dst = nnsToAccess(hadoopConf);
        FileSystem dstFs = dst.getFileSystem(hadoopConf);
        dstFs.addDelegationTokens(getTokenRenewer(hadoopConf), creds);
        return creds;

    }

    private String getTokenRenewer(Configuration conf) throws IOException {
        UserGroupInformation renewer = null;
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        if ( currentUser.getRealUser() == null) {
            renewer = currentUser;
        } else {
            renewer = currentUser.getRealUser();
        }
        return renewer.getShortUserName();
    }


    private Path nnsToAccess(Configuration hadoopConf) throws Exception {
        Path homedir = null;
        try {
            homedir = FileSystem.get(hadoopConf).getHomeDirectory();
        } catch (IOException e) {
            String msg = String.format ("Error in getting filesystem path %s", e.getMessage());
            LOG.error(msg, e);
            throw new Exception(e.getMessage());
        }
        return homedir;
    }

}
