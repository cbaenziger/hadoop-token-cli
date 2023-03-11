# Hadoop Token CLI

The Hadoop Delegation Token CLI (DTC) is a CLI based on the Hadoop Tool class to provide Hadoop delegation tokens which provide a means to authenticate to Hadoop clusters as an alternative to using Kerberos for authentication. This is built using Hadoop 3 dependencies for HDFS, (HTTP/HTTPS) WebHDFS, HBase and Hive.

## Usage

To use this CLI one needs a keytab with Hadoop proxy rights for the service(s) desired and an environment setup with standard configuration directory environment variables.

To run the command looks like:
```
$ java -cp hadoop-token-cli-0.01-SNAPSHOT-jar-with-dependencies.jar com.bloomberg.hi.dtclient.DTGatherer
Usage: dtgatherer <keytab principal> <keytab file> <token file> <requested user> <service1> [<service2>, ...]
```

For example:
```
$ java -cp hadoop-token-cli-0.01-SNAPSHOT-jar-with-dependencies.jar -Dlog4j.configuration=file://`pwd`/log4j.properties com.bloomberg.hi.dtclient.DTGatherer proxy_user@DEV.EXAMPLE.COM `pwd`/proxy_user.keytab test_token cbaenziger hdfs webhdfs hbase
main 2023-02-21 03:47:02 INFO  DTGatherer:35 - Using principal: proxy_user@DEV.EXAMPLE.COM, keytab: /home/cbaenzig/proxy_user.keytab, for user cbaenzig, writing file test_token
main 2023-02-21 03:47:03 INFO  DTGatherer:60 - Requesting service hdfs
main 2023-02-21 03:47:05 INFO  DTGatherer:60 - Requesting service webhdfs
main 2023-02-21 03:47:06 INFO  DTGatherer:60 - Requesting service hbase
$ echo $?
0
$ hadoop dtutil print  test_token
File: test_token
Token kind               Service              Renewer         Exp date     URL enc token
--------------------------------------------------------------------------------
HBASE_AUTH_TOKEN         abcde926-b313-497a-a57d-abcde99d9726 -NA-            -NA-         [...]
WEBHDFS delegation       1.2.3.4:9870         proxy_user   2/21/23 4:47 AM [...]
HDFS_DELEGATION_TOKEN    ha-hdfs:hadoop-lab01 proxy_user   2/21/23 4:47 AM [...]
```
