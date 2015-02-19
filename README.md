# parquet-thrift-cascalog

Idiomatic Cascalog taps to read and write Parquet Thrift files.

## Writing

When writing, the thrift class must be specified. Pass a tap of thrift
objects and they'll be written in Parquet format.

```clojure
(ns example.core
    (:require [parquet-thrift-cascalog.core :refer [hfs-parquet]])
    (:import [parquet.thrift.cascalog.test Name]))

(?- (hfs-parquet path :thrift-class Name)
    [name1 name2])
```

## Reading

In the simplest case, just pass the path to the tap. The thrift
definition class must be available on the classpath but you don't have
to specify it.

```clojure
(ns example.core
    (:require [parquet-thrift-cascalog.core :refer [hfs-parquet]]))

(?- (stdout)
    (hfs-parquet path))
```

You can specify the thrift class if you want, it won't hurt anything.

```clojure
(ns example.core
    (:require [parquet-thrift-cascalog.core :refer [hfs-parquet]])
    (:import [parquet.thrift.cascalog.test Name]))

(?- (stdout)
    (hfs-parquet path :thrift-class Name))
```

### Predicate Pushdown

Parquet supports the concept of predicate pushdown. You can provide a
predicate that Parquet runs while iterating over your records. Parquet
keeps some simple statistics on the blocks it writes, so predicates
can skip whole sections of records without deserialization. Big
performance win.

You can define filters using the parquet.filter2.predicate.FilterApi
class's static methods. Be careful to match the types of your Thrift
schema with the values you provide in the filters. Things like
long/int mismatches will cause exceptions.

```clojure
(ns example.core
    (:require [parquet-thrift-cascalog.core :refer [hfs-parquet]])
    (:import [parquet.filter2.predicate FilterApi]))

(def id-is-1 (FilterApi/eq (FilterApi/intColumn "id") (int 1))

(?- (stdout)
    (hfs-parquet path :filter id-is-1))
```

Adding a Clojure filter DSL is on the roadmap for this library.

### Projection

Parquet also supports projections (in the relational algebra
sense). Many data jobs require only a subset of an objects fields. For
example, if we wanted only the `id` and `first_name` fields of a
larger Name object, we could pass a projection string to specify the
fields we cared about.

```clojure
(ns example.core
    (:require [parquet-thrift-cascalog.core :refer [hfs-parquet]]))

(?- (stdout)
    (hfs-parquet path :projection "id;first_name"))
```

A more complex projection string and the fields it'd include:

`"a/**;b;c/*;d/{e,f}"`

* All fields under a
* b
* c's direct children
* e and f under d

Unprojected fields' behavior depends on the Thrift schema. `optional`
fields are just dropped. `required` fields are initialized to some
kind of type aware empty value. 0 for ints, empty string for strings,
etc.

## License

Copyright © 2015 Alex Robbins

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
