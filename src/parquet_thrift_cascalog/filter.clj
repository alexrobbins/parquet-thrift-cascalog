(ns parquet-thrift-cascalog.filter
  "Clojure wrapper over the Parquet filter interface."
  (:refer-clojure :exclude [and or not])
  (:require [clojure.tools.macro :as m])
  (:import [java.nio ByteBuffer]
           [parquet.filter2.predicate FilterApi Operators$Column]
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

;; No one has to know there aren't string columns.
(defn string-column
  [^String col-path]
  (FilterApi/binaryColumn col-path))

(defprotocol ParquetValue
  (parquet-value [s]
    "Converts values into Parquet appropriate types."))

(extend-protocol ParquetValue
  (Class/forName "[B")
  (parquet-value [s] (Binary/fromByteArray s))

  ByteBuffer
  (parquet-value [s] (Binary/fromByteBuffer s) )

  String
  (parquet-value [s] (Binary/fromString s))

  Object
  (parquet-value [s] s)

  nil
  (parquet-value [s] nil))

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
  ;; When passed a column, `column` generates a column of the same
  ;; type with the new column path.
  Operators$Column
  (column [c s]
    (let [f (column-impl (.getColumnType c))]
      (f nil s)))

  Binary
  (column [_ s] (binary-column s))

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
  "Asserts that the supplied argument is a string or a column. Used to
  verify the first argument to the predicate generators."
  [col-path method-name]
  (assert (clojure.core/or (string? col-path)
                           (isa? (class col-path) Operators$Column))
          (format
           "The first argument to %s must be a string representing the column name or a column. It is a %s"
           method-name
           (class col-path))))

(defmacro defpred [& method-names]
  `(do ~@(for [m method-names :let [m-name (name m)]]
           `(defn ~m
              {:arglists '([~'col-path ~'value])}
              [col-path# value#]
              (assert-path! col-path# ~m-name)
              (let [coerced-v# (parquet-value value#)]
                (if (isa? (class col-path#) Operators$Column)
                  (. FilterApi ~m col-path# coerced-v#)
                  (. FilterApi ~m (column coerced-v# col-path#) coerced-v#)))))))

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
