(ns parquet-thrift-cascalog.filter
  "Clojure wrapper over the Parquet filter interface."
  (:refer-clojure :exclude [and or not])
  (:import [parquet.filter2.predicate FilterApi]
           [parquet.io.api Binary]))

;; Columns

(defn int-column
  [^String col-path]
  (FilterApi/intColumn col-path))

(defn long-column
  [^String col-path]
  (FilterApi/longColumn col-path))

(defn float-column
  [^String col-path]
  (FilterApi/floatColumn col-path))

(defn double-column
  [^String col-path]
  (FilterApi/doubleColumn col-path))

(defn boolean-column
  [^String col-path]
  (FilterApi/booleanColumn col-path))

(defn binary-column
  [^String col-path]
  (FilterApi/binaryColumn col-path))


;; Predicates

(defn eq
  [column value]
  (FilterApi/eq column value))

(defn notEq
  [column value]
  (FilterApi/notEq column value))

(defn lt
  [column value]
  (FilterApi/lt column value))

(defn ltEq
  [column value]
  (FilterApi/ltEq column value))

(defn gt
  [column value]
  (FilterApi/gt column value))

(defn gtEq
  [column value]
  (FilterApi/gtEq column value))

(defn and
  [pred1 pred2]
  (FilterApi/and pred1 pred2))

(defn or
  [pred1 pred2]
  (FilterApi/or pred1 pred2))

(defn not
  [pred]
  (FilterApi/not pred))


;; Comparison helpers

(defn string->binary
  [string]
  (Binary/fromString string))