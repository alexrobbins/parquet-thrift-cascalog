(ns parquet-thrift-cascalog.core-test
  (:require [clojure.test :refer :all]
            [cascalog.cascading.io :as io]
            [cascalog.api :refer :all]
            [cascalog.logic.testing :refer :all]
            [parquet-thrift-cascalog.core :refer :all]
            [parquet-thrift-cascalog.filter :as f :refer [pred]])
  (:import [parquet.thrift.cascalog.test Address
                                         Name
                                         Person]))

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
    (let [id-pred (pred (= "id" (int 1)))
          id-pred-2 (pred (> "id" (int 1)))
          string-pred (pred (= "first_name" "A"))
          nil-pred (pred (= (f/binary-column "last_name") nil))
          nil-pred-2 (pred (= (f/string-column "last_name") nil))]
      (io/with-fs-tmp [_ tmp]
        (?- (hfs-parquet tmp :thrift-class Name)   ;; write names
            names)
        (test?<- [(first names)]
                 [?name]
                 ((hfs-parquet tmp :filter id-pred) ?name))
        (test?<- (rest names)
                 [?name]
                 ((hfs-parquet tmp :filter id-pred-2) ?name))
        (test?<- [(first names)]
                 [?name]
                 ((hfs-parquet tmp :filter string-pred) ?name))
        (test?<- [(last names)]
                 [?name]
                 ((hfs-parquet tmp :filter nil-pred) ?name))
        (test?<- [(last names)]
                 [?name]
                 ((hfs-parquet tmp :filter nil-pred-2) ?name))))))

(defn make-person
  [id fname lname & [street zip]]
  (let [name (Name. id fname)
        _    (.setLast_name name lname)
        person (Person. name)]
    (when street
      (let [address (Address. street)]
        (when zip (.setZip address zip))
        (.setAddress person address)))
    person))

(def people
  (mapv #(apply make-person %)
        [[1 "A" "AL"]
         [2 "B" "BL" "Street"]
         [2 "C" "CL" "Street" "Zip"]]))

(deftest nested-schema-predicate-test
  (let [street-pred (pred (= (f/string-column "address.street") nil))
        zip-pred (pred (= (f/string-column "address.zip") nil))]
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-parquet tmp :thrift-class Person)   ;; write people
          people)
      (test?<- [(first people)]
               [?person]
               ((hfs-parquet tmp :filter street-pred) ?person))
      (test?<- (subvec people 0 2)
               [?person]
               ((hfs-parquet tmp :filter zip-pred) ?person)))))

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

(def ones (constantly 1))

(deftest outfields-test
  (io/with-log-level :fatal
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-parquet tmp :thrift-class Name :outfields "?name") ;; write names
          (<- [?name ?one]
              (names ?name)
              (ones ?name :> ?one)))
      (test?<- names
               [?name]
               ((hfs-parquet tmp) ?name)))))