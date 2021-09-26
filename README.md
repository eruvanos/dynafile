> Consider the project of a proof of concept! Definitely not production ready!

# Dynafile

Embedded pure Python NoSQL database following DynamoDB concepts.

```bash

pip install dynafile

# with string filter support using filtration

pip install "dynafile[filter]"

# bloody edge

pip install git+https://github.com/eruvanos/dynafile.git
pip install filtration

```

## Overview

Dynafile stores items within partitions, which are stored as separate files. Each partition contains a SortedDict from `sortedcontainers` which are sorted by the sort key attribute.

Dynafile does not implement the interface or functionality of DynamoDB, 
but provides familiar API patterns.

Differences:

- Embedded, file based
- No pagination
- No file limits

## Features

- persistence
- put item
- get item
- delete item
- scan - without parameters
- query - starts_with
- query - index direction
- query - filter

## Roadmap

- scan - filter
- global secondary index
- update item
- batch write
- batch get
- thread safeness
- event stream hooks
- local secondary index
- split partitions
- parallel scans - pre defined scan segments
- transactions

## API

```python
from dynafile import *

db = Dynafile(path=".", pk_attribute="PK", sk_attribute="SK")
db.put_item(
    item={
        "PK": "user#1",
        "SK": "user#1",
        "name": "Bob",
    }
)
db.put_item(
    item={
        "PK": "user#1",
        "SK": "role#1",
        "TYPE": "sender",
    }
)
db.put_item(
    item={
        "PK": "user#2",
        "SK": "user#2",
        "name": "Alice",
    }
)

item = db.get_item(key={
    "PK": "user#1",
    "SK": "user#1"
})
# will provide single item identified by pk and sk

items = list(db.query(pk="user#1"))
# will provide item collection of pk=user#1 

list(db.scan())
# will provide all items in db

```


### Filter

`query` and `scan` support filter, you can provide callables as filter like lambda expressions.

Another option are [filtration](https://pypi.org/project/filtration/) expressions.

* Equal ("==")
* Not equal ("!=")
* Less than ("<")
* Less than or equal ("<=")
* Greater than (">")
* Greater than or equal (">=")
* Contains ("in")
    * RHS must be a list or a Subnet
* Regular expression ("=~")
    * RHS must be a regex token

Examples:
* `SK =~ /^a/` - SK starts with a
* `SK == 1` - SK is equal 1
* `SK == 1` - SK is equal 1
* `nested.a == 1` - accesses nested structure `item.nested.a`