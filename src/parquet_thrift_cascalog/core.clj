(ns parquet-thrift-cascalog.core
  (:require [cascalog.api :refer [hfs-tap lfs-tap]])
  (:import [parquet.cascading ParquetTBaseScheme
                              ParquetValueScheme$Config]))

(defn- parquet-scheme
  "Custom scheme for dealing with Parquet files. Must provide thrift
   class to write. When reading, the thrift class can be read from the
   metadata but the class must be available on the classpath."
  ([] (ParquetTBaseScheme. ))
  ([{:keys [thrift-class filter projection]}]
     (let [config (ParquetValueScheme$Config.)
           config (if thrift-class
                    (.withRecordClass config thrift-class)
                    config)
           config (if filter
                    (.withFilterPredicate config filter)
                    config)
           config (if projection
                    (.withProjectionString config projection)
                    config)]
       (ParquetTBaseScheme. config))))

(defn hfs-parquet
  "Read a Parquet file from Hadoop fs.

  Returns a Cascading Hfs tap with support for the supplied scheme,
  opened up on the supplied path or file object. Supported keyword
  options are:

  `:thrift-class` - The thrift class to use to read or write. Optional
  when used as a source, required when used as a sink. When used as a
  source the class must be available on the classpath.

  `:filter` - Parquet filter. Used for Parquet's predicate pushdown.
  See parquet.filter2.predicate.FilterApi for examples.

  `:projection` - Projection string. Used to materialize a subset of
  the records' fields. "
  [path & opts]
  (let [opts-map (apply array-map opts)]
    (apply hfs-tap
           (parquet-scheme opts-map)
           path opts)))

(defn lfs-parquet
  "Read a Parquet file from the local fs.

  Returns a Cascading Hfs tap with support for the supplied scheme,
  opened up on the supplied path or file object. Supported keyword
  options are:

  `:thrift-class` - The thrift class to use to read or write. Optional
  when used as a source, required when used as a sink. When used as a
  source the class must be available on the classpath.

  `:filter` - Parquet filter. Used for Parquet's predicate pushdown.
  See parquet.filter2.predicate.FilterApi for examples.

  `:projection` - Projection string. Used to materialize a subset of
  the records' fields. "
  [path & opts]
  (let [opts-map (apply array-map opts)]
    (apply lfs-tap
           (parquet-scheme opts-map)
           path opts)))