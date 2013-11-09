# -*- coding: utf-8 -*-
"""
"""
import datetime
import urllib3

from pprint import pprint as pp
from termcolor import colored as cl
import logging

from schema import Schema

from clj import dumps, loads
import json
from itertools import izip


class DB(object):

  def __init__(self, host, port, store, db, schema=None, **kwargs):
    """ Assuming the datomic REST service was started this way:

    > bin/rest -p 8888 mem datomic:mem://

    Then get a connection for database name 'test' like this:

    >>> db = DB("localhost", 8888, "mem", "test", schema=S)

    """
    self.host, self.port, self.store, self.db  = host, port, store, db
    self.uri_str = "/data/"+ self.store +"/"
    self.uri_db  = "/data/"+ self.store +"/"+ self.db +"/"
    self.uri_q   = "/api/query"
    self.pool    = urllib3.connectionpool.HTTPConnectionPool(
        self.host, port=self.port,
        timeout=3, maxsize=20,
        headers={"Accept":"application/edn", "Connection": "Keep-Alive"})
    "debugging"
    for d in ('debug_http','debug_loads'):
      setattr(self, d, kwargs.get(d) == True)
    "build or use provided Schema"
    if isinstance(schema, Schema):
      self.schema = schema
    elif isinstance(schema, tuple):
      self.schema = Schema(schema)
    else:
      self.schema = None
      if schema is None: return
      logging.warning("I don't know what to do with schema kwarg of type '%s'" % type(schema))

  def create(self):
    """ Creates the database
    >>> db.create()
    True
    """
    data = data={"db-name":self.db}
    self.rest('POST', self.uri_str, status_codes=(200,201), data=data)
    return True

  def info(self):
    """ Fetch the current db state
    >>> db.info()
    {:db/alias "store/db", :basis-t ...}
    """
    return self.rest('GET', self.uri_db + '-/')
  
  def tx_schema(self, **kwargs):
    """ Builds the data structure edn, and puts it in the db
    """
    for s in self.schema.schema: 
      tx = self.tx(s, **kwargs)

  def tx(self, *args, **kwargs):
    """ Executes a raw tx string, or get a new TX object to work with.

    Passing a raw string or list of strings will immedately transact 
    and return the API response as a dict.
    >>> resp = tx('{:db/id #db/id[:db.part/user] :person/name "Bob"}')
    {db-before: db-after: tempids: }

    This gets a fresh `TX()` to prepare a transaction with.
    >>> tx = db.tx()

    New `E()` object with person/fname and person/lname attributes
    >>> person = tx.add('person/',   {'fname':'John', 'lname':'Doe'})

    New state and city objects referencing the state
    >>> state  = tx.add('loc/state', 'WA')
    >>> city   = tx.add('loc/city',  'Seattle', 'isin', state)

    Add person/city, person/state, and person/likes refs to the person entity
    >>> person.add('person/', {'city': city, 'state': state, 'likes': [city, state]})

    Excute the transaction
    >>> resp = tx.tx()        

    The resolved entity ids for our person
    >>> person.eid, state.eid, city.eid

    Fetch all attributes, behave like a dict
    >>> person.items()
    >>> person.iteritems()

    Access attribute as an attribute
    >>> person['person/name']

    See `TX()` for options.

    """
    if 0 == len(args): return TX(self) 
    ops = []
    for op in args:
      if isinstance(op, list):            ops += op
      elif isinstance(op, (str,unicode)): ops.append(op)
    if 'debug' in kwargs: pp(ops)
    tx_proc ="[ %s ]" % "".join(ops)
    x = self.rest('POST', self.uri_db, data={"tx-data": tx_proc})
    return x
  
  def e(self, eid):
    """Get an Entity
    """
    ta = datetime.datetime.now()
    rs = self.rest('GET', self.uri_db + '-/entity', data={'e':int(eid)}, parse=True)
    tb =  datetime.datetime.now() - ta
    print cl('<<< fetched entity %s in %sms' % (eid, tb.microseconds/1000.0), 'cyan')
    return rs

  def retract(self, e, a, v):
    """ redact the value of an attribute
    """
    ta = datetime.datetime.now()
    ret = u"[:db/retract %i :%s %s]" % (e, a, dump_edn_val(v))
    rs = self.tx(ret)
    tb = datetime.datetime.now() - ta
    print cl('<<< retracted %s,%s,%s in %sms' % (e,a,v, tb.microseconds/1000.0), 'cyan')
    return rs


  def datoms(self, index='aevt', e='', a='', v='', 
                   limit=0, offset=0, chunk=100, 
                   start='', end='', since='', as_of='', history='', **kwargs):
    """ Returns a lazy generator that will only fetch groups of datoms
        at the chunk size specified.

    http://docs.datomic.com/clojure/index.html#datomic.api/datoms
    """
    assert index in ['aevt','eavt','avet','vaet'], "non-existant index"
    data = {'index':   index, 
            'a':       ':{0}'.format(a) if a else '',
            'v':       dump_edn_val(v) if v else '',
            'e':       int(e) if e else '', 
            'offset':  offset or 0,
            'start':   start,
            'end':     end,
            'limit':   limit,
            'history': 'true' if history else '',
            'as-of':   int(as_of) if as_of else '',
            'since':   int(since) if since else '',
            }
    data['limit'] = offset + chunk
    rs = True
    while rs and (data['offset'] < (limit or 1000000000)):
      ta = datetime.datetime.now()
      rs = self.rest('GET', self.uri_db + '-/datoms', data=data, parse=True)
      if not len(rs):
        rs = False
      tb = datetime.datetime.now() - ta
      print cl('<<< fetched %i datoms at offset %i in %sms' % (
        len(rs), data['offset'], tb.microseconds/1000.0), 'cyan')
      for r in rs: yield r
      data['offset'] += chunk

  def rest(self, method, uri, data=None, status_codes=None, parse=True, **kwargs):
    """ Rest helpers
    """
    r = self.pool.request_encode_body(method, uri, fields=data, encode_multipart=False)
    if not r.status in (status_codes if status_codes else (200,201)):
      print cl('\n---------\nURI / REQUEST TYPE : %s %s' % (uri, method), 'red')
      print cl(data, 'red')
      print r.headers
      raise Exception, "Invalid status code: %s" % r.status
    if not parse: 
      " return raw urllib3 response"
      return r
    if not self.debug_loads:
      " return parsed edn"
      return loads(r.data)
    "time edn parse time and return parsed edn"
    return self.debug(loads, args=(r_data, ), kwargs={},
          fmt='<<< parsed edn datastruct in {ms}ms', color='green')

  def debug(self, defn, args, kwargs, fmt=None, color='green'):
    """ debug timing, colored terminal output
    """
    ta = datetime.datetime.now()
    rs = defn(*args, **kwargs)  
    tb = datetime.datetime.now() - ta
    fmt = fmt or "processed {defn} in {ms}ms"
    logmsg = fmt.format(ms=tb.microseconds/1000.0, defn=defn)
    "terminal output"
    print cl(logmsg, color)
    "logging output"
    logging.debug(logmsg)
    return rs

  def q(self, q, inputs=None, limit='', offset='', history=False):
    """ query
    """
    if not q.strip().startswith("["): q = "[ {0} ]".format(q)
    args     = u'[ {:db/alias "%(store)s/%(db)s" %(hist)s} %(inputs)s ]' % dict(
      store  = self.store,
      db     = self.db,
      hist   = ':history true' if history==True else '',
      inputs = " ".join(inputs or []))
    data = {"args":   args, 
            "q":      q, 
            "offset": offset or '',
            "limit":  limit  or '',
            }
    return self.rest('GET', self.uri_q, data=data, parse=True)

  def find(self, *args, **kwargs):
    " new query builder on current db"
    return Query(*args, db=self, schema=self.schema)
    





class Query(object):
  """ chainable query builder"
  
    >>> db.find('?e ?a') # default find
    >>> q.where()        # with add
    >>> q.ins()          # in   add
  """
  
  def __init__(self, find, db=None, schema=None):
    self.db       = db
    self.schema   = schema
    self._find    = []
    self._where   = []
    self._input   = []
    self._limit   = None
    self._offset  = None
    self._history = False
    self.find(find)
  
  def __repr__(self):
    return " ".join([str(self._find), str(self._in), str(self._where)])
  
  def find(self, *args, **kwargs):
    " :find "
    if args[0] is all:
      pass # finds all
    else:
      [(self._find.append(x)) for x in args]
    return self
  
  def where(self, *args, **kwargs):
    " :where "
    [(self._where.append(x)) for x in args]
    return self

  def fulltext(self, attr, s, q, e, v):
    self._where.append("(fulltext $ {0} {1}) [[{2} {3}]]".format(attr, s, e, v))
    self._input.append((s, q))

  def param(self, *args, **kwargs):
    " :in   "
    for first, second in pairwise(args):
      if isinstance(second, list):
        if not isinstance(second[0], list):
          " add a logical _or_ " 
          self._input.append((
            u"[{0} ...]".format(first), second)) 
        else:
          " relations, list of list"
          self._input.append((
            u"[[{0}]]".format(first), second)) 
      elif isinstance(second, tuple):
        " tuple "
        self._input.append((
          u"[{0}]".format(first), list(second))) 
      else:
        " nothing special "
        self._input.append((first,second)) 
    return self

  def limit(self, limit):
    self._limit = limit
    return self
  def offset(self, offset):
    self._offset = offset
    return self
  def history(self, history):
    self._offset = history
    return self

  def hashone(self):
    "execute query, get back"
    rs = self.one()
    if not rs: 
      return {}
    else:
      finds = " ".join(self._find).split(' ')
      return dict(zip((x.replace('?','') for x in finds), rs))
  
  def one(self):
    "execute query, get a single list"
    self.limit(1)
    rs = self.all()
    if not rs: 
      return None
    else:
      return rs[0]

  def all(self):
    " execute query, get all list of lists"
    query,inputs = self._toedn()
    return self.db.q(query,
      inputs  = inputs,
      limit   = self._limit,
      offset  = self._offset,
      history = self._history)
  
  def _toedn(self):
    """ prepare the query for the rest api
    """
    finds  = u""
    inputs = u""
    wheres = u""
    args   = []
    ": in and args"
    for a,b in self._input:
      inputs += " {0}".format(a)
      args.append(dump_edn_val(b))
    if inputs:
      inputs = u":in ${0}".format(inputs)
    " :where "
    for where in self._where:
      if isinstance(where, (str,unicode)): 
        wheres += u"[{0}]".format(where)
      elif isinstance(where, (list)):
        wheres += u" ".join([u"[{0}]".format(w) for w in where])
    " find: "
    if self._find == []: #find all
      fs = set()
      for p in wheres.replace('[',' ').replace(']',' ').split(' '):
        if p.startswith('?'):
          fs.add(p)
      self._find = list(fs)
    finds = " ".join(self._find)
    " all togethr now..."
    q = u"""[ :find {0} {1} :where {2} ]""".\
        format( finds, inputs, wheres)
    return q,args





class E(dict):
  """ An entity and its db, optionally a tx.
  """
  def __init__(self, e, db=None, tx=None):
    """ Represents an entity in the db,
    or a tempid in a non-committed state.
   
    >>> person = E(1, db)
    >>> person.eid
    1

    Fetch all attributes, behave like a dict
    >>> person.items()

    Iterator just like a dictionary
    >>> person.iteritems()

    Access attribute as an attribute
    >>> person['person/name']
    >>> person.get('person/name')

    Access ns attribute with dot notation
    >>> person.person

    """

    assert (db is not None or tx is not None),\
        "A DB or TX object is required"

    self._eid   = int(e)
    self._db    = db or tx.db
    self._tx    = tx
    self._txid  = -1 if not tx else tx.txid
    self._dict  = None

  def __repr__(self):
    return "{'db/id': %s}" % cl(self._eid, 'magenta')
  def __unicode__(self):
    return u"#db/id[:db.part/user %s]" % self._eid
  def __int__(self):
    return self._eid

  """ compare entity id + at-tx
  """
  def __eq__(self, obj):
    if not isinstance(obj, E): return False
    return self._eid  == obj._eid and \
           self._txid == obj._txid
  def __ne__(self, obj):
    if not isinstance(obj, E): return True
    return self._eid  != obj._eid or \
           self._txid != obj._txid

  """ compare at-tx
  """
  def __lt__(self, obj):
    return self._txid < obj._txid
  def __gt__(self, obj):
    return self._txid > obj._txid
  def __le__(self, obj):
    return self._txid <= obj._txid
  def __ge__(self, obj):
    return self._txid >= obj._txid
  
  """ attributes
  """
  @property
  def __dict__(self):
    'returns a dictionary with last known state in the db'
    if isinstance(self._dict, dict): return self._dict
    if self._eid < 0:                return {} # uncommitted
    self._dict = self._db.e(self.eid)     # fetch
    return self._dict

  def vpar(self, val):
    # TODO - check schema for type,cardinality 
    if not isinstance(val, dict): return val
    return E(val.get('db/id'), db=self._db, tx=self._tx)

  def __getitem__(self, attr, default=None):
    val = self.__dict__.get(attr, default)
    return self.vpar(val)

  def __getattr__(self, attr, default=None):
    val = self.__dict__.get(attr, default)
    if val: return self.vpar(v)

    rs, ns = {}, '{0}/'.format(attr)
    for k,v in self.__dict__.iteritems():
      if k.startswith(ns):
        attr = "/".join(k.split('/')[1:])
        vp = self.vpar(v)
        if not attr in rs:
          rs[attr] = vp
        elif isinstance(rs[attr], list):
          rs[attr].append(vp)
        else:
          rs[attr] = list(rs[attr], vp)
    return rs

  @property
  def items(self):
    return self.__dict__.items
  @property
  def iteritems(self):
    return self.__dict__.iteritems

  @property
  def eid(self):
    return self._eid

  def add(self, *args, **kwargs):
    self._tx.add(self, *args, **kwargs)

  def retract(self, a, v):
    assert self.eid > 0, "unresolved entity state, cannot issue retractions"
    if not a.startswith(':'): 
      a = u':%s' % v
    self._db.tx(u'[:db/retract {0} {1} {2}]'.\
                format(self.eid, a, dump_edn_val(v)))












class TX(object):
  """ Accumulate, execute, and resolve tempids
  """
  def __init__(self, db):
    self.db = db
    self.tmpents, self.adds, self.ctmpid, self.txid = [], [], -1, -1
    self.resp     = None
    self.realents = []

  def __repr__(self):
    return "<datomic tx, %i pending>" % len(self)

  def __len__(self):
    return len(self.adds or [])

  def __int__(self):
    return self.txid

  def add(self, *args, **kwargs):
    """ Accumulate datums for the transaction

    Start a transaction on an existing db connection
    >>> tx = TX(db)

    Get get an entity object with a tempid
    >>> ref = add()
    >>> ref = add(0)
    >>> ref = add(None)
    >>> ref = add(False)

    Entity id passed as first argument (int|long)
    >>> tx.add(1, 'thing/name', 'value')

    Shorthand form for multiple attributes sharing a root namespace
    >>> tx.add(':thing/', {'name':'value', 'tag':'value'})

    Attributes with a value of None are ignored
    >>> tx.add(':thing/ignored', None)

    Add multiple datums for an attribute with carinality:many
    >>> tx.add(':thing/color', ['red','white','blue'])

    """

    assert self.resp is None, "Transaction already committed"
    entity, av_pairs, args = None, [], list(args)
    if len(args):
      if isinstance(args[0], (int, long)): 
        " first arg is an entity or tempid"
        entity = E(args[0], tx=self)
      elif isinstance(args[0], E):
        " dont resuse entity from another tx"
        if args[0]._tx is self:
          entity  = args[0]
        else:
          if int(args[0]) > 0:
            " use the entity id on a new obj"
            entity = E(int(args[0]), tx=self)
          args[0] = None
      " drop the first arg"
      if entity is not None or args[0] in (None, False, 0):
        v = args.pop(0)
    " auto generate a temp id?"
    if entity is None:
      entity       = E(self.ctmpid, tx=self)
      self.ctmpid -= 1
    " a,v from kwargs"
    if len(args) == 0 and kwargs: 
      for a,v in kwargs.iteritems():
        self.addeav(entity, a, v)
    " a,v from args "
    if len(args):
      assert len(args) % 2 == 0, "imbalanced a,v in args: " % args
      for first, second in pairwise(args):
        if not first.startswith(':'): 
          first = ':' + first
        if not first.endswith('/'):
          " longhand used: blah/blah "
          if isinstance(second, list):
            for v in second:
              self.addeav(entity, first, v)
          else:
            self.addeav(entity, first, second)
          continue
        elif isinstance(second, dict):
          " shorthand used: blah/, dict "
          for a,v in second.iteritems():
            self.addeav(entity, "%s%s" % (first, a), v)
            continue
        elif isinstance(second, (list, tuple)):
          " shorthand used: blah/, list|tuple "
          for a,v in pairwise(second):
            self.addeav(entity, "%s%s" % (first, a), v)
            continue
        else:
          raise Exception, "invalid pair: %s : %s" % (first,second)
    "pass back the entity so it can be resolved after tx()"
    return entity
  
  def execute(self, **kwargs):
    """ commit the current statements from add()
    """
    assert self.resp is None, "Transaction already committed"
    try:
      self.resp = self.db.tx(list(self.edn_iter), **kwargs)
    except Exception:
      self.resp = False
      raise
    else:
      self.resolve()
      self.adds = None
      self.tmpents = None
    return self.resp # raw dict response

  def resolve(self):
    """ Resolve one or more tempids. 
    Automatically takes place after transaction is executed.
    """
    assert isinstance(self.resp, dict), "Transaction in uncommitted or failed state"
    rids = [(v) for k,v in self.resp['tempids'].items()]
    self.txid = self.resp['tx-data'][0]['tx']
    rids.reverse()
    for t in self.tmpents:
      pos = self.tmpents.index(t)
      t._eid, t._txid = rids[pos], self.txid
    for t in self.realents:
      t._txid = self.txid
  
  def addeav(self, e, a, v):
    if v is None: return
    self.adds.append((e, a, v))
    if int(e) < 0 and e not in self.tmpents:
      self.tmpents.append(e)
    elif int(e) > 0 and e not in self.realents:
      self.realents.append(e)

  @property
  def edn_iter(self):
    """ yields edns
    """
    for e,a,v in self.adds:
      yield u"{%(a)s %(v)s :db/id #db/id[:db.part/user %(e)s ]}" % \
            dict(a=a, v=dump_edn_val(v), e=int(e))




def dump_edn_val(v):
  " edn simple value dump"
  if isinstance(v, (str, unicode)): 
    return json.dumps(v)
  elif isinstance(v, E):            
    return unicode(v)
  else:                             
    return dumps(v)

def pairwise(iterable):
  "s -> (s0,s1), (s2,s3), (s4, s5), ..."
  a = iter(iterable)
  return izip(a, a)
