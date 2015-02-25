(ns parquet-thrift-cascalog.filter
  "Clojure wrapper over the Parquet filter interface."
  (:refer-clojure :exclude [and or not])
  (:require [clojure.tools.macro :as m])
  (:import [parquet.filter2.predicate FilterApi Operators$Column]
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

(defprotocol Column
  (column [v col-path] "Takes the value that the supplied column path
  will be compared against and the column path, and returns a column
  of the proper type."))

(defn column-impl
  "Returns the column implementation for the supplied class. Hacky."
  [klass]
  (-> (meta #'column) :protocol deref
      :impls
      (get klass)
      :column))

(extend-protocol Column
  (Class/forName "[B")
  (column [_ s] (binary-column s))

  ;; Generates a column of the same type with the new path.
  Operators$Column
  (column [c s]
    (let [f (column-impl (.getColumnType c))]
      (f nil s)))

  Integer
  (column [_ s] (int-column s))

  Long
  (column [_ s] (long-column s))

  Float
  (column [_ s] (float-column s))

  Double
  (column [_ s] (double-column s))

  Boolean
  (column [_ s] (boolean-column s)))

;; Predicates

(defn assert-path!
  "Asserts that the supplied argument is a string. Used to verify the
  first argument to the predicate generators."
  [col-path method-name]
  (assert (string? col-path)
          (format
           "The first argument to %s must be a string representing the column name."
           method-name)))

(defmacro defpred [& method-names]
  `(do ~@(for [m method-names :let [m-name (name m)]]
           `(defn ~m
              {:arglists '([~'col-path ~'value])}
              [col-path# value#]
              (assert-path! col-path# ~m-name)
              (. FilterApi ~m (column value# col-path#) value#)))))

(defpred eq notEq lt ltEq gt gtEq)

(defn and
  [pred1 pred2]
  (FilterApi/and pred1 pred2))

(defn or
  [pred1 pred2]
  (FilterApi/or pred1 pred2))

(defn not
  [pred]
  (FilterApi/not pred))

(defmacro pred
  "Macro that converts forms declared within its body into Parquet
  predicates."
  [& forms]
  `(m/symbol-macrolet
    [~'= eq
     ~'not= notEq
     ~'< lt
     ~'<= ltEq
     ~'> gt
     ~'>= gtEq
     ~'and and
     ~'or or
     ~'not not]
    ~@forms))

;; Comparison helpers

(defn string->binary
  [^String string]
  (Binary/fromString string))
