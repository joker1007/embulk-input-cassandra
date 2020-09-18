# Cassandra input plugin for Embulk
![Java CI](https://github.com/joker1007/embulk-input-cassandra/workflows/Java%20CI/badge.svg)

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Caution
In current, version of netty components conflicts to one that is used by embulk-core.

This probrem is very severe.

I tested this plugin on embulk-0.9.22.
But future embulk version may break this plugin.

## Support Data types

| CQL Type                    | Embulk Type                              | Descritpion                           |
| --------                    | -----------                              | --------------                        |
| ascii                       | string                                   | use `toString` or `toJson`            |
| bigint                      | long                                     |                                       |
| blob                        | unsupported                              |                                       |
| boolean                     | boolean                                  |                                       |
| counter                     | long                                     |                                       |
| date                        | timestamp                                |                                       |
| decimal                     | string                                   | use `toString`                        |
| double                      | double                                   |                                       |
| float                       | double                                   |                                       |
| inet                        | string                                   | use toHostAddress()                   |
| int                         | string, boolean(as 0 or 1), long, double | overflowed value is reset to 0        |
| list                        | json                                     |                                       |
| map (support only text key) | json                                     |                                       |
| set                         | json                                     |                                       |
| smallint                    | long                                     | overflowed value is reset to 0        |
| text                        | string                                   | use `toString` or `toJson`            |
| time                        | long                                     | use millisecond from beginning of day |
| timestamp                   | timestamp                                | long and double as epoch second       |
| timeuuid                    | string                                   |
| uuid                        | string                                   |
| varchar                     | string                                   | use `toString` or `toJson`            |
| varint                      | long                                     |                                       |
| UDT, tuple                  | unsupported                              |                                       |

## Configuration

- **hosts**: list of seed hosts (list<string>, required)
- **port**: port number for cassandra cluster (integer, default: `9042`)
- **username**: cluster username (string, default: `null`)
- **password**: cluster password (string, default: `null`)
- **cluster_name**: cluster name (string, default: `null`)
- **keyspace**: target keyspace name (string, required)
- **table**: target table name (string, required)
- **concurrency**: Thread count of fetcher (integer, default: count of processors)
- **select**: select column names (list<string>, default: "all columns")
- **connect_timeout**: Set connect timeout millisecond (integer, default: `5000`)
- **request_timeout**: Set each request timeout millisecond (integer, default: `12000`)

Input schema is detected automatically from schema of Cassandra.

## Example

```yaml
in:
  type: cassandra
  keyspace: embulk_test
  table: "test_basic"
  concurrency: 8
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
