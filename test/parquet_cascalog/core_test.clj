(ns parquet-cascalog.core-test
  (:require [clojure.test :refer :all]
            [cascalog.cascading.io :as io]
            [cascalog.api :refer :all]
            [midje.cascalog :refer :all]
            [midje.sweet :refer :all]
            [parquet-cascalog.core :refer :all]))

#_(deftest roundtrip-test
  (fact
    (io/with-fs-tmp [_ tmp]
      (?- (hfs-parquet tmp)   ;; write line
          [["Proin,hendrerit,tincidunt pellentesque"]])
      (fact "Test round trip with hfs-delimited"
        (<- [?a ?b ?c]
            ((hfs-parquet tmp :delimiter ",") ?a ?b ?c)) =>
        (produces [["Proin" "hendrerit" "tincidunt pellentesque"]])))))