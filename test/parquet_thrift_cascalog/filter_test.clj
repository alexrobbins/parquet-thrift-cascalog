(ns parquet-thrift-cascalog.core-test
  (:require [clojure.test :refer :all]
            [parquet-thrift-cascalog.filter :as f]))

(deftest pred-generation-test
  (testing "the pred form translates its guts into Parquet filters."
    (is (= (f/and (f/not (f/lt "face" 10))
                  (f/gtEq "face" 13))
           (f/pred
            (let [column-name "face"]
              (and (not (< column-name 10))
                   (>= column-name 13))))))))

(deftest column-creation-test
  (let [original (f/column 10 "col")
        cloned (f/column original "new-name")]
    (is (= Long
           (.getColumnType original)
           (.getColumnType cloned))
        "The cloned column has the same type as the original..")
    (is (= "col" (.toDotString (.getColumnPath original))))
    (is (= "new-name" (.toDotString (.getColumnPath cloned)))))  )
