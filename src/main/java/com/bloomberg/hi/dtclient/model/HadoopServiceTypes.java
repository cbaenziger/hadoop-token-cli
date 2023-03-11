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

package com.bloomberg.hi.dtclient.model;

public enum HadoopServiceTypes {
    HDFS("hdfs"),
    HBASE("hbase"),
    HIVE("hive"),
    WEBHDFS("webhdfs");

    private final String name;

    private HadoopServiceTypes(String s) {
        name = s;
    }

    public static HadoopServiceTypes fromExternal(String s){
      for(HadoopServiceTypes t : HadoopServiceTypes.values()){
        if( t.name.equalsIgnoreCase(s)){
          return t;
        }
      }
      return null;
    }

    public boolean equalsName(String otherName) {
        return name.equals(otherName);
    }

    public String toString() {
        return this.name;
    }
}
