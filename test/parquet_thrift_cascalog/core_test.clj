(ns parquet-thrift-cascalog.core-test
  (:require [clojure.test :refer :all]
            [cascalog.cascading.io :as io]
            [cascalog.api :refer :all]
            [cascalog.logic.testing :refer :all]
            [parquet-thrift-cascalog.core :refer :all]
            [parquet-thrift-cascalog.filter :as f])
  (:import [parquet.thrift.cascalog.test Address
                                         Name
                                         TestPerson]))

(defn make-name [id fname & [lname]]
  (let [name (Name. id fname)]
    (when lname
      (.setLast_name name lname))
    name))

(deftest roundtrip-test
  (io/with-log-level :fatal
    (let [name (make-name 1 "First name" "Last name")]
      (io/with-fs-tmp [_ tmp]
        (?- (hfs-parquet tmp :thrift-class Name)   ;; write name
            [name])
        (test?<- [name]
                 [?name]
                 ((hfs-parquet tmp) ?name))))))

(defn make-names [& names]
  (mapv #(apply make-name %) names))

(def names
  (make-names
   [1 "A" "Lastname"]
   [2 "B" "Lastname"]
   [3 "C" nil]))

(deftest filter-test
  (io/with-log-level :fatal
    (let [id-pred (f/eq (f/int-column "id") (int 1)) ;; without coercion this fails as a long.
          string-pred (f/eq (f/binary-column "first_name") (f/string->binary "A"))
          nil-pred (f/eq (f/binary-column "last_name") nil)]
      (io/with-fs-tmp [_ tmp]
        (?- (hfs-parquet tmp :thrift-class Name)   ;; write names
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

(deftest projection-test
  (io/with-log-level :fatal
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-parquet tmp :thrift-class Name)   ;; write names
          names)
      (test?<- (make-names [1 "A"] [2 "B"] [3 "C"])
               [?name]
               ((hfs-parquet tmp :projection "id;first_name") ?name))
      (test?<- (make-names [1 ""] [2 ""] [3 ""])
               [?name]
               ((hfs-parquet tmp :projection "id") ?name))
      (test?<- (make-names [0 "" "Lastname"] [0 "" "Lastname"] [0 ""])
               [?name]
               ((hfs-parquet tmp :projection "last_name") ?name)))))