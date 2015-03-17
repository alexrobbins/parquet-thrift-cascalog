namespace java parquet.thrift.cascalog.test

struct Name {
  1: required i32 id,
  2: required string first_name,
  3: optional string last_name
}

struct Address {
  1: required string street,
  2: optional string zip
}

struct Person {
  1: required Name name,
  3: optional Address address,
}
