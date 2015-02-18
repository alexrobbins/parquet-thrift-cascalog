(ns parquet-thrift-cascalog.core-test
  (:require [clojure.test :refer :all]
            [cascalog.cascading.io :as io]
            [cascalog.api :refer :all]
            [cascalog.logic.testing :refer :all]
            [parquet-thrift-cascalog.core :refer :all])
  (:import [parquet.thrift.cascalog.test Address
                                         Name
                                         TestPerson]))

(deftest roundtrip-test
  (io/with-log-level :fatal
    (let [name (doto (Name. 23 "First name")
                 (.setLast_name "Last name"))]
      (io/with-fs-tmp [_ tmp]
        (?- (hfs-parquet tmp :thrift-class Name)   ;; write line
            [name])
        (test?<- [name]
                 [?name]
                 ((hfs-parquet tmp) ?name))))))

(deftest filter-test
  (io/with-log-level :fatal
    (let [name (doto (Name. 23 "First name")
                 (.setLast_name "Last name"))
          ;; xxx Setup age filter
          ;; string filter for first name using binary column
          ]
      (io/with-fs-tmp [_ tmp]
        (?- (hfs-parquet tmp :thrift-class Name)   ;; write line
            [name])
        (test?<- [name]
                 [?name]
                 ((hfs-parquet tmp) ?name))))))