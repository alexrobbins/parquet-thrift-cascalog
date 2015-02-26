(defproject parquet-thrift-cascalog "0.1.3-SNAPSHOT"
  :description "Idiomatic Cascalog taps for reading Thrift-specified Parquet data."
  :url "https://github.com/alexrobbins/parquet-thrift-cascalog"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.twitter/parquet-cascading "1.6.0rc4"]
                 [org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.macro "0.1.2"]
                 [cascalog "2.1.1"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]
                                  [cascalog/midje-cascalog "2.1.1"]]
                   :plugins [[lein-thriftc "0.2.1"]
                             [lein-midje "3.1.3"]]
                   :hooks [leiningen.thriftc]
                   :thriftc {:source-paths ["test/thrift"]}}
             :provided {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]
                                       [org.apache.thrift/libthrift "0.9.2"
                                        :exclusions [org.slf4j/slf4j-api]]]}})
