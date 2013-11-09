datomic-py
==========

A Python library for the Datomic REST API. 

While nothing will ever match the speed or power of Datomic's Clojure interface, this library will work well for the basics.


```shell
pip install datomic-py
```

Connect
=======


```python
from datomic import *

host   = 'localhost'
port   = 8888
store  = 'mem'
dbname = 'test'
db     = DB(host, port, store, dbname, schema=None)


# create the database
db.create() 
True

# get the state
db.info()  
{'basis-t': 62, 'db/alias': 'mem/test'}

```


Schema
======

You do not need to define a schema. If you want to, it is just a bunch of nested tuples.

Unless specified otherwise, each attribute is assumed to a string with a cardinality of one.

```python

S=(
   ('person',
     ('name',   FULL,               "A Persons name"),
     ('email',  FULL,               "A Person's email"),
     ('age',    LONG,               "A Person's age"),
     ('likes',  REF, MANY, ISCOMP,  "A Persons likes"),
   ),
   ('item',
     ('name',   FULL),
     ('sku',    UNIQ),
     ('active', BOOL),
     ('cat',    MANY, ENUM('cat','dog','pony','horse','gerbil','sloth')),
   ),
   ('review',
     ('person', REF),
     ('item',   REF),
     ('stars',  LONG),
   ),
)
```

You can pass the schema to DB when connecting.

```python

db = DB(host, port, store, dbname, schema=S)


# get the schema data
db.schema.schema

['{:db/id #db/id[:db.part/db]
 :db/ident :person/name
 :db/fulltext true
 :db/doc "A Persons name"
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db.install/_attribute :db.part/db}',
 '...',
]

# transact the schema
db.tx_schema()


```

For a more comprehensive schema example, see [datomic/datomic_test.py](datomic/datomic_test.py)




Transact
========

```python
tx = db.tx()

person = tx.add("person/", {
  'name':   "John Doe" , 
  'age':    25,
  })

item = tx.add("item/", {
  'name':    'Item 1',  
  'sku':     'item-1-sku',
  'active':  True,
  'cat':     ['cat','dog'],
  })

person.add('person/likes', item)

review  = tx.add("review/", {
  'item':    item, 
  'stars':   4, 
  'person':  person, 
  })

review2 = tx.add("review/", {
  'item':    item, 
  'stars':   5, 
  'person':  tx.add("person/", {
                'name': 'Nested Person',
                'age':  22, 
    }),
  })

print person, item, review, review2
{'db/id': -1} {'db/id': -2} {'db/id': -4} {'db/id': -6}

tx.execute()
{'db-after':  {'basis-t': 1042, 'db/alias': 'mem/test'}, 
 'db-before': {'basis-t': 1040, 'db/alias': 'mem/test'}, 
 'tx-data':   [{'a': 50, 'added': True, 'e': 13194139534354, 'tx': 13194139534354, 'v': datetime.datetime(2013, 11, 9, 18, 55, 56, 657000, tzinfo=<UTC>)}, {'....'}]
}

print person, item, review, review2
{'db/id': 17592186045459} {'db/id': 17592186045460} {'db/id': 17592186045462} {'db/id': 17592186045464}

print person.eid, item.eid, review.eid, review2.eid
17592186045459 17592186045460 17592186045462 17592186045464

print unicode(person)
#db/id[:db.part/user 17592186045459] 

```



Entity
======

```python
db.e(person)
{'person/age': 25, 'person/likes': ({'item/name': 'Item 1', 'item/sku': 'item-1-sku', 'item/cat': set(['dog', 'cat']), 'item/active': True, 'db/id': 17592186045460},), 'db/id': 17592186045459, 'person/name': 'John Doe'}

db.e(item.eid)
{'item/name': 'Item 1', 'item/sku': 'item-1-sku', 'item/cat': set(['dog', 'cat']), 'item/active': True, 'db/id': 17592186045460}

db.e(17592186045462)
{'review/person': {'db/id': 17592186045459}, 'review/stars': 4, 'db/id': 17592186045462, 'review/item': {'db/id': 17592186045460}}
```



Query
=====

```python
p_name  = '?e :person/name  ?n'
p_age   = '?e :person/age   ?a'
p_email = '?e :person/email ?m'


# get one
db.find('?e ?n').where(p_name).one()
[17592186045457, 'John Doe']


# one to dict
p = db.find('?e ?n').where(p_name,p_age).hashone()
p.items()
[('e', 17592186045457), ('n', 'John Doe')]


# OR input param
qa = db.find('?e ?n ?a').where(p_name, p_age)\
       .param('?n', ['Nested Person', 'John Doe'])
qa.all()
[[17592186045463, 'Nested Person', 22], [17592186045459, 'John Doe', 25]]

qa.limit(1).all()
[[17592186045463, 'Nested Person', 22]]


# AND input param
qb = db.find('?e ?n ?a').where(p_name, p_age)\
       .param('?n ?a', ('John Doe', 25))
qb.all()
[17592186045459, 'John Doe', 25]]


# unify external data
qc = db.find('?e ?n ?external').where(p_name, p_age)\
       .param('?n ?external', 
            [ ['John Doe', 123.23], ['Nested Person', 456.00]])
qc.all()
[[17592186045459, 'John Doe', 123.23], [17592186045463, 'Nested Person', 456.0]]

```



Retract
=======

```python
db.e(review2).get('review/stars')
5

db.retract(review2, 'review/stars', 5)

db.e(review2)
{'review/person': {'db/id': 17592186045463}, 'db/id': 17592186045464, 'review/item': {'db/id': 17592186045460}}

```



Datums
======

db.datums() lazily fetches datums in the chunk size you specify.

```python

for r in db.datoms('aevt', a='person/name', limit=100, chunk=100):
  print r
{'a': 62, 'added': True, 'e': 17592186045459, 'tx': 13194139534354, 'v': 'John Doe'}
{'a': 62, 'added': True, 'e': 17592186045463, 'tx': 13194139534354, 'v': 'Nested Person'}
```

```python
for r in db.datoms('avet', a='item/sku', v='item-1-sku', limit=100):
  print r
{'a': 67, 'added': True, 'e': 17592186045460, 'tx': 13194139534354, 'v': 'item-1-sku'}
```



Dependencies
============

* [edn-format]() - in my testing this was the fastest parser
* [urllib3]()    - connection pooling provides great performance
* [termcolor]()  - for colors in the terminal when debugging/testing




TODO
====

* A python library for the C++ edn parser is in progress and should be more performant than 

* More tests

* Better support for traversing the graph

* Eager loading of entities

* Materialized Views
