namespace java parquet.thrift.cascalog.test

struct Name {
  1: required i32 id,
  2: required string first_name,
  3: optional string last_name
}

struct Address {
  1: string street,
  2: required string zip
}

struct TestPerson {
  1: required Name name,
  2: optional i32 age,
  3: Address address,
  4: string info
}
