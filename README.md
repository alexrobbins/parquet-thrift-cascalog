# parquet-thrift-cascalog

Idiomatic Cascalog taps to read and write Parquet Thrift files.

## Latest Version

The latest release version of parquet-thrift-cascalog is hosted on [Clojars](https://clojars.org):

[![Clojars Project](http://clojars.org/parquet-thrift-cascalog/latest-version.svg)](http://clojars.org/parquet-thrift-cascalog)

## Writing

When writing, the Thrift class must be specified. Pass a tap of Thrift
objects and they'll be written in Parquet format.

```clojure
(ns example.core
    (:require [parquet-thrift-cascalog.core :refer [hfs-parquet]])
    (:import [parquet.thrift.cascalog.test Name]))

(?- (hfs-parquet path :thrift-class Name)
    [name1 name2])
```

`hfs-parquet` can only handle a single field per output tuple. This
isn't a problem if your query only has one output field. However, your
tuple might have several fields to provide values for a templatetap, or
because the query is used in several places. Specify the field
containing your Thrift object with `:outfields`.

```clojure
(ns example.core
    (:require [parquet-thrift-cascalog.core :refer [hfs-parquet]])
    (:import [parquet.thrift.cascalog.test Name]))

(?- (hfs-parquet path :thrift-class Name
                      :outfields "?name"
                      :templatefields "?shard"
                      :sink-template "%s")
    (<- [?name ?shard]
        (names ?name)
        (name->shard ?name :> ?shard)))
```

This example query writes to subfolders specified by `?shard`, but
the output in those folders is only the Thrift object.

## Reading

In the simplest case, just pass the path to the tap. The Thrift
definition class must be available on the classpath but you don't have
to specify it.

```clojure
(ns example.core
    (:require [parquet-thrift-cascalog.core :refer [hfs-parquet]]))

(?- (stdout)
    (hfs-parquet path))
```

You can specify the Thrift class if you want, it won't hurt anything.

```clojure
(ns example.core
    (:require [parquet-thrift-cascalog.core :refer [hfs-parquet]])
    (:import [parquet.thrift.cascalog.test Name]))

(?- (stdout)
    (hfs-parquet path :thrift-class Name))
```

### Predicate Pushdown

Parquet supports predicate pushdown. You can provide a predicate
that Parquet runs while iterating over your records. Parquet
keeps some simple statistics on the blocks it writes, so predicates
can skip whole sections of records without deserialization. Big
performance win.

Use the functions in `parquet-thrift-cascalog.filter` to set up your
predicates.  Be careful to match the types of your Thrift schema with
the values you provide in the filters. Things like long/int mismatches
will cause exceptions.

```clojure
(ns example.core
    (:require [parquet-thrift-cascalog.core :refer [hfs-parquet]]
              [parquet-thrift-cascalog.filter :as f]))

(def id-is-1 (f/eq (f/int-column "id") (int 1)) ;;coerce to avoid int/long mismatch

(?- (stdout)
    (hfs-parquet path :filter id-is-1))
```

#### Nils

* nil can only be passed to eq or notEq.
* nil is eq to nil and notEq to everything else.
* All other predicates drop rows with nil, since nil isn't `Comparable`.

#### Strings

Parquet represents strings as binary columns. When using a string as a
filter value use the `string->binary` helper function to get the
string into a `Comparable` binary format.

```clojure
(ns example.core
    (:require [parquet-thrift-cascalog.core :refer [hfs-parquet]]
              [parquet-thrift-cascalog.filter :as f]))

(def fname-is-alex (f/eq (f/binary-column "first_name") (string->binary "alex"))

(?- (stdout)
    (hfs-parquet path :filter fname-is-alex))
```

### Projection

Parquet also supports projections (in the relational algebra
sense). Many data jobs require only a subset of an object's fields. For
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
