(ns parquet-thrift-cascalog.core
  (:require [cascalog.api :refer [hfs-tap lfs-tap]]
            [cascalog.cascading.util :refer [fields]])
  (:import [cascading.tuple Fields]
           [parquet.cascading ParquetTBaseScheme
                              ParquetValueScheme$Config]))

(defn- parquet-scheme
  "Custom scheme for dealing with Parquet files. Must provide thrift
   class to write. When reading, the thrift class can be read from the
   metadata but the class must be available on the classpath."
  ([] (ParquetTBaseScheme. ))
  ([{:keys [thrift-class filter projection outfields] :or {outfields Fields/ALL}}]
     (let [config (ParquetValueScheme$Config.)
           config (if thrift-class
                    (.withRecordClass config thrift-class)
                    config)
           config (if filter
                    (.withFilterPredicate config filter)
                    config)
           config (if projection
                    (.withProjectionString config projection)
                    config)
           scheme (ParquetTBaseScheme. config)
           sink-fields (fields outfields)]
       (.setSinkFields scheme sink-fields)
       scheme)))

(defn hfs-parquet
  "Read a Parquet file from Hadoop fs.

  Returns a Cascading Hfs tap with support for the supplied scheme,
  opened up on the supplied path or file object. Supported keyword
  options are:

  `:thrift-class` - The thrift class to use to read or write. Optional
  when used as a source, required when used as a sink. When used as a
  source the class must be available on the classpath.

  `:filter` - Parquet filter. Used for Parquet's predicate pushdown.
  Use parquet-thrift-cascalog.filter to set these up.

  `:projection` - Projection string. Used to materialize a subset of
  the records' fields.

  `:outfields` - The field containing thrift objects. Not needed if
  there is only one field. Often used in conjunction with
  `templatefields` and `sink-template`.

  Also supports all the hfs-tap keyword options:

  `:sinkmode` - can be `:keep`, `:update` or `:replace`.

  `:sinkparts` - used to constrain the segmentation of output files.

  `:source-pattern` - Causes resulting tap to respond as a GlobHfs tap
  when used as source.

  `:sink-template` - Causes resulting tap to respond as a TemplateTap when
  used as a sink.

  `:templatefields` - When pattern is supplied via :sink-template,
  this option allows a subset of output fields to be used in the
  naming scheme."
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
  Use parquet-thrift-cascalog.filter to set these up.

  `:projection` - Projection string. Used to materialize a subset of
  the records' fields.

  `:outfields` - The field containing thrift objects. Not needed if
  there is only one field. Often used in conjunction with
  `templatefields` and `sink-template`.

  Also supports all the lfs-tap keyword options:

  `:sinkmode` - can be `:keep`, `:update` or `:replace`.

  `:sinkparts` - used to constrain the segmentation of output files.

  `:source-pattern` - Causes resulting tap to respond as a GlobHfs tap
  when used as source.

  `:sink-template` - Causes resulting tap to respond as a TemplateTap when
  used as a sink.

  `:templatefields` - When pattern is supplied via :sink-template,
  this option allows a subset of output fields to be used in the
  naming scheme."
  [path & opts]
  (let [opts-map (apply array-map opts)]
    (apply lfs-tap
           (parquet-scheme opts-map)
           path opts)))