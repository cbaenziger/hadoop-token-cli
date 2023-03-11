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
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.Collection;

public class WebHdfsTokenProvider extends HadoopTokenProvider {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public Credentials obtainCredentials(Configuration hadoopConf) throws Exception {
        String base64TokenString = null;
        Credentials tmpCreds = new Credentials();

        // try https first and fall back to http if we fail to connect
        try {
            WebHdfsFileSystem webHdfsFileSystem = getActiveFileSystem(hadoopConf, true);
            webHdfsFileSystem.addDelegationTokens(getTokenRenewer(hadoopConf), tmpCreds);
        } catch (IOException e) {
            WebHdfsFileSystem webHdfsFileSystem = getActiveFileSystem(hadoopConf, false);
            webHdfsFileSystem.addDelegationTokens(getTokenRenewer(hadoopConf), tmpCreds);
        }

        Collection<Token<? extends TokenIdentifier>> tokens = tmpCreds.getAllTokens();
        for (Token<? extends TokenIdentifier> token : tokens) {
            if ((token.getKind().equals(WebHdfsConstants.WEBHDFS_TOKEN_KIND)) ||
                    (token.getKind().equals(WebHdfsConstants.SWEBHDFS_TOKEN_KIND))) {
                base64TokenString = Base64.getEncoder().encodeToString(token.encodeToUrlString().getBytes());
            }
        }
        return tmpCreds; //base64TokenString;
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

    private WebHdfsFileSystem getActiveFileSystem(Configuration hadoopConf, Boolean secure) throws Exception {
        WebHdfsFileSystem webFs = new WebHdfsFileSystem();
        URI hdfsURI = URI.create(hadoopConf.getTrimmed("fs.defaultFS"));
        URI serviceURI = new URI(String.format("%s://%s", (secure ? WebHdfsConstants.SWEBHDFS_SCHEME : WebHdfsConstants.WEBHDFS_SCHEME), hdfsURI.getHost()));
        log.debug("Trying to connect to %s", serviceURI);
        webFs.initialize(serviceURI, hadoopConf);
        /*WebHdfsFileSystem webFs = null;
        String errorMessage = "Unable to find the given namenode";
        try {
            Map<String, InetSocketAddress> namenodes = DFSUtil.getHaNnWebHdfsAddresses(hadoopConf, WebHdfsFileSystem.SCHEME).
                    get(DFSUtil.getNamenodeNameServiceId(hadoopConf));

            for (Map.Entry<String, InetSocketAddress> namenode : namenodes.entrySet()) {
                String namenodeAddr = String.format("%s:%s", namenode.getValue().getHostName(), namenode.getValue().getPort());
                String namenodeUri = WebHdfsFileSystem.SCHEME + String.format("://%s", namenodeAddr);
                webFs = (WebHdfsFileSystem) FileSystem.get(new URI(namenodeUri), hadoopConf);
                try {
                    webFs.getFileStatus(new Path("/tmp"));
                    //We found the webhdfs a.k.a the correct namenode which is active.
                    //No need to waste time looping and trying the second or third one...
                    break;
                } catch ( AuthorizationException e) {
                    throw new Exception(e.getMessage());
                }
                 catch (IOException e ) {
                    errorMessage = e.getMessage();
                    webFs = null;
                }
            }
        }catch (URISyntaxException e ) {
            String message = String.format("Error in parsing of webhdfs uri %s", e.getMessage());
            LOG.error(message, e);
            throw new Exception(message);

        }
        //Have we got a valid webhdfs instance. If not then raise an exception
        if (webFs == null) {
            throw new Exception(errorMessage);
        }*/
        return webFs;
    }
}
