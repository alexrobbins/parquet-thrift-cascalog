(defproject parquet-cascalog "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-thriftc "0.2.1"]]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [cascalog "2.1.1"]
                 [com.twitter/parquet-cascading "1.6.0rc4"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]
                                  [cascalog/midje-cascalog "2.1.1"]]}
             :provided {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]]}})
