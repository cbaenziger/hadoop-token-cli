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

package com.bloomberg.hi.dtclient.managers;

import org.apache.hadoop.security.Credentials;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.bloomberg.hi.dtclient.model.HadoopServiceTypes;

import com.bloomberg.hi.dtclient.factory.TokenProviderFactory;
import com.bloomberg.hi.dtclient.provider.HadoopTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;

public class HadoopTokenRequestManager {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private File keytab;
    private String keyTabUserName;
    private UserGroupInformation keyTabUserUserGroupInformation;
    private HadoopTokenProvider tokenProvider;
    private Configuration conf;

    public HadoopTokenRequestManager(String principal, String keytabFile) {
        this.conf = conf;
        this.keyTabUserName = principal;
        this.keytab = new File(keytabFile);
    }

    // getters and setters
    public File getKeytab() {
        return keytab;
    }

    public void setKeytab(File keyTab) {
        this.keytab = keyTab;
    }

    protected String getKeyTabUserName() {
        return keyTabUserName;
    }

    public void setKeyTabUserName(String keyTabUserName) {
        this.keyTabUserName = keyTabUserName;
    }

    public HadoopTokenProvider getTokenProvider() {return tokenProvider;}

    public void setTokenProvider(HadoopTokenProvider tokenProviderFactory) {this.tokenProvider = tokenProviderFactory;}

    public Credentials fetchHadoopToken(String proxyUser, Configuration conf, String service) throws Exception {
        if (null != keyTabUserUserGroupInformation) {
            refreshKeyTabUserUserGroupInformation();
        } else {
            loginWithKeytabAndSetKeyTabUgi();
        }
        return new TokenProviderFactory().fetchCredentials(
                HadoopServiceTypes.fromExternal(service),
                proxyUser,
                this.refreshKeyTabUserUserGroupInformation(),
                conf
        );
    }

    private void loginWithKeytabAndSetKeyTabUgi() throws Exception {
        log.debug("Starting getKeyTabUserUserGroupInformation");
        this.keyTabUserUserGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                keyTabUserName,
                keytab.getAbsolutePath()
        );
        log.debug("We have set hadoopConf");
        log.debug (String.format("Logging in using %s", keyTabUserName));
        log.debug (String.format("Logging in using keytab %s", keytab.getAbsolutePath()));
        if (!keyTabUserUserGroupInformation.isFromKeytab() && !keyTabUserUserGroupInformation.hasKerberosCredentials()) {
            throw new Exception("Unable to logon using keberos keytab");
        }
        log.debug(String.format("Is from keytab: %s Has kerb credentials: %s", keyTabUserUserGroupInformation.isFromKeytab(), keyTabUserUserGroupInformation.hasKerberosCredentials()));
    }

    private UserGroupInformation refreshKeyTabUserUserGroupInformation() throws Exception {

        log.debug("Starting getKeyTabUserUserGroupInformation");
        keyTabUserUserGroupInformation.checkTGTAndReloginFromKeytab();
        log.debug(String.format("Is from keytab: %s Has kerb credentials: %s", keyTabUserUserGroupInformation.isFromKeytab(), keyTabUserUserGroupInformation.hasKerberosCredentials()));
        return keyTabUserUserGroupInformation;
    }
}
