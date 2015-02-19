(ns parquet-thrift-cascalog.core-test
  (:require [clojure.test :refer :all]
            [cascalog.cascading.io :as io]
            [cascalog.api :refer :all]
            [cascalog.logic.testing :refer :all]
            [parquet-thrift-cascalog.core :refer :all])
  (:import [parquet.thrift.cascalog.test Address
                                         Name
                                         TestPerson]
           [parquet.filter2.predicate FilterApi]
           [parquet.io.api Binary]))

(deftest roundtrip-test
  (io/with-log-level :fatal
    (let [name (doto (Name. 1 "First name")
                 (.setLast_name "Last name"))]
      (io/with-fs-tmp [_ tmp]
        (?- (hfs-parquet tmp :thrift-class Name)   ;; write line
            [name])
        (test?<- [name]
                 [?name]
                 ((hfs-parquet tmp) ?name))))))

(defn make-names [& names]
  (mapv (fn [[id fname lname]]
          (let [name (Name. id fname)]
            (when lname
              (.setLast_name name lname))
            name))
        names))

(deftest filter-test
  (io/with-log-level :fatal
    (let [id-pred (FilterApi/eq (FilterApi/intColumn "id") (int 1)) ;; without coercion this fails as a long.
          string-pred (FilterApi/eq (FilterApi/binaryColumn "first_name") (Binary/fromString "A"))
          nil-pred (FilterApi/eq (FilterApi/binaryColumn "last_name") nil)
          names (make-names
                 [1 "A" "Lastname"]
                 [2 "B" "Lastname"]
                 [3 "C" nil])]
      (io/with-fs-tmp [_ tmp]
        (?- (hfs-parquet tmp :thrift-class Name)   ;; write line
            names)
        (test?<- [(first names)]
                 [?name]
                 ((hfs-parquet tmp :filter id-pred) ?name))
        (test?<- [(first names)]
                 [?name]
                 ((hfs-parquet tmp :filter string-pred) ?name))
        (test?<- [(last names)]
                 [?name]
                 ((hfs-parquet tmp :filter nil-pred) ?name))))))
