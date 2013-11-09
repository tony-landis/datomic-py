# -*- coding: utf8 -*-
"""
"""

from datomic import *
from schema import *
import datetime
from pprint import pprint as pp


S=(
   ('person',
     ('name',   FULL,               "A Persons name"),
     ('email',  FULL,               "A Person's email"),
     ('thumb',  BYTES,              "A Person's avatar"),
     ('age',    LONG,               "A Person's age"),
     ('passwd', NOHIST,             "A Person's password"),
     ('likes',  REF, MANY,          "A Persons likes"),
     ('view',   REF, MANY, ISCOMP,  "A Person's viewed items"),
   ),
   ('view',
     ('when',   INSTANT),
     ('from',   URI),
     ('item',   REF),
   ),
   ('order',
     ('uid',    UUID, INDEX),
     ('date',   INSTANT, INDEX),
     ('user',   REF),
     ('track',  BIGINT),
     ('item',   REF, MANY, ISCOMP),
     ('tax',    FLOAT),
     ('ship',   DOUBLE),
     ('total',  BIGDEC),
     ('sent',   BOOL),
     ('idx',    INDEX),
   ),
   ('item',
     ('name',   FULL),
     ('desc',   FULL),
     ('sku',    UNIQ),
     ('active', BOOL),
     ('amt',    FLOAT),
     ('cat',    MANY, ENUM('cat','dog','pony','horse','gerbil','sloth')),
   ),
   ('review',
     ('user',   REF),
     ('item',   REF),
     ('strs',   LONG),
     ('when',   INSTANT),
   )
)


HOST  = 'localhost'
PORT  = 8888
STORE = 'mem'
DBN   = 'pytest3'

db  = DB(HOST, PORT, STORE, DBN, S, 
         debug_http  = True,
         debug_loads = False)



def test_all():


  " db creation "
  created = db.create()

  " schema creation "
  schemers = db.tx_schema() #debug=True)

  " transact "
  tx2 = db.tx()

  people = []

  person = tx2.add("person/", {
    'name':   "A User" , 
    'age':    25,
    'passwd': 'password123',
    })

  item1 = tx2.add("item/", {
    'name':    'Cat House',  
    'sku':     'sku-1-%s' % datetime.datetime.now(),       
    'active':  True,
    'amt':     99.99, 
    'desc':    'some description',
    'cat':     'cat',
    })
  
  tx2.add("order/", {
    'user':    person, 
    'item':    item1, 
    'idx':     'test-idx', 
    })

  tx2.add("review/", {
    'user':    person, 
    'item':    item1, 
    'strs':    3, 
    })

  item2 = tx2.add("item/", {
    'name':    'Dog House',  
    'sku':     'sku-3-%s' % datetime.datetime.now(),       
    'active':  True,
    'amt':     199.99, 
    'desc':    'some description',
    'cat':     'dog',
    })

  rev2 = tx2.add("review/", {
    'item':    item2, 
    'strs':    5, 
    'user':    tx2.add("person/", {
                  'name': 'Nested Person',
                  'age':  22, }), 
               })

  rs2 = tx2.execute() #debug=True)
  assert rs2, "TX failed"

  tx3 = db.tx()
  person2 = tx3.add(person,  "person/email", "tony.landis@gmail.com")
  person3 = tx3.add(person2, "person/likes", [item1, item2])
  rs3 = tx3.execute() #debug=True)
  assert rs3, "TX failed"

  # auto-resolving
  assert person.eid > 1
  assert int(item1) > 1

  # entity + at-tx comparision operators
  assert person == person
  assert person != person2

  # at-tx comparision
  assert person  < person2
  assert person2 > person

  assert person  >= person
  assert person  <= person

  assert person2 >= person
  assert person  <= person2

  # acts like a dict
  print person2.items()
  print person2

  # more dictish behavior
  for k,v in rev2.iteritems():
    print k,v

  # dict access to full attribute
  assert rev2['review/strs'] == 5

  # property style access to a root attr namespace
  ns = rev2.review
  assert ns['strs'] == 5
  
  # entity conversion
  assert isinstance(ns['item'], E)
  assert isinstance(ns['user'], E)

  # walking the tree
  assert ns['item'].eid == item2.eid
  assert ns['user']['person/name'] == 'Nested Person'
  assert ns['user']['person/age']  == 22
  assert isinstance(ns['user']['db/id'], int)

  # retract
  rs = db.retract(rev2, 'review/strs', 5)
  assert rs

  p_name = '?e :person/name ?n'
  p_age  = '?e :person/age ?a'
  p_pass = '?e :person/passwd ?p'

  pname = lambda *a: '{0} :person/name {1}'.format(*a)

  # ONE
  one = db.find('?e ?n').where(p_name).one()
  assert isinstance(one, list)
  assert one[0]
  assert one[1]
  
  # HASH ONE
  one = db.find('?e ?n').where(p_name).hashone()
  assert isinstance(one, dict)
  assert one['e']
  assert one['n']

  # OR input param
  qa = db.find('?e ?n ?a').where(p_name, p_age)\
         .param('?n', ['Nested Person', 'A User'])

  # AND input param
  qb = db.find('?e ?n ?a ?p').where(p_name, p_age, p_pass)\
         .param('?n ?p ?a', ('A User', 'password123', 25))
  
  # EXTERNAL join
  qc = db.find('?e ?n ?external').where(p_name, p_age, p_pass)\
         .param('?e', person.eid)\
         .param('?n ?external', 
              [ ['A User', 123.23], ['Nested Person', 456]])

  pp(qa.limit(2).all())
  pp(qb.limit(2).all())
  pp(qc.limit(1000).all())

  # find all notations
  qd = db.find(all).where(pname('?e','?r'), p_age, p_pass)
  pp(qd.limit(2).all())

  # fulltext
  qf = db.find('?e ?name ?a').where(p_age)
  qf.fulltext(':person/name', '?search', 'user', '?e', '?name') 
  pp(qf.limit(10).all())
  
  # datums
  for r in db.datoms('aevt', a='person/name', limit=100):
    print r
  for r in db.datoms('avet', a='order/idx', v='test-idx', limit=100):
    print r


if __name__ == '__main__':
  test_all()
