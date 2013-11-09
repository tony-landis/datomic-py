""" SCHEMA

Required schema attributes default to:

:db/cardinality 

  ONE       [default]
  MANY

:db/valueType

  STRING    [default]
  BOOLEAN
  LONG
  BIGINT
  FLOAT
  DOUBLE
  BIGDEC
  REF
  INSTANT
  UUID
  URI
  BYTES

:db/unique

  UNIQ   [:db.unqiue/value]
  IDENT  [:db.unqiue/identity]

:db/isComponent
    
  SCOMP

:db/noHist
      
  NOHIST

"""

VALUETYPE   = ':db/valueType'
CARDINALITY = ':db/cardinality'
UNIQUE      = ':db/unique'
INDEX       = ':db/index'
FULLTEXT    = ':db/fulltext'
ISCOMPONENT = ':db/isComponent'
NOHISTORY   = ':db/noHistory' 


STRING   = (VALUETYPE,    ':db.type/string')
KEYWORD  = (VALUETYPE,    ':db.type/keyword')
BOOL     = (VALUETYPE,    ':db.type/boolean')
LONG     = (VALUETYPE,    ':db.type/long')
BIGINT   = (VALUETYPE,    ':db.type/bigint')
FLOAT    = (VALUETYPE,    ':db.type/float')
DOUBLE   = (VALUETYPE,    ':db.type/double')
BIGDEC   = (VALUETYPE,    ':db.type/bigdec')
REF      = (VALUETYPE,    ':db.type/ref')
INSTANT  = (VALUETYPE,    ':db.type/instant')
UUID     = (VALUETYPE,    ':db.type/uuid')
URI      = (VALUETYPE,    ':db.type/uri')
BYTES    = (VALUETYPE,    ':db.type/bytes')
ONE      = (CARDINALITY,  ':db.cardinality/one')
MANY     = (CARDINALITY,  ':db.cardinality/many')
UNIQ     = (UNIQUE,       ':db.unique/value')
IDENT    = (UNIQUE,       ':db.unique/identity')
INDEX    = (INDEX,        'true')
FULL     = (FULLTEXT,     'true')
ISCOMP   = (ISCOMPONENT,  'true')
NOHIST   = (NOHISTORY,    'true')
ENUM     = lambda *x:x



class Schema(object):
  """
  An example schema.
  """

  part   = 'db'
  schema = []
  cache  = None

  def __init__(self, struct, part=None):
    """
    Pythonic schemas for Datomic
    """
    self.cache  = {}
    if part: self.part = part
    self.build_attributes(struct)


  """ Schema Preparation
  """
  def build_attributes(self, struct):
    for outer in struct:
      for row in outer:
        if type(row) in (str,unicode):
          ns = row
          #setattr(self.tx, ns, Ns(ns, self.tx))
        elif type(row) in (list,tuple):
          self.build_attribute(ns, row)
          #setattr(getattr(self.tx, ns), row[0], None)
        else:
          raise Exception, 'Invalid schema definition at row %s' % row

  def build_attribute(self, ns, struct):
    attrs, enums = [],[]
    attrs.append((':db/id', '#db/id[:db.part/%s]' % self.part))
    attrs.append((':db/ident', ":%s%s%s" % (ns, '/' if (ns and struct[0]) else '', struct[0]) ))
    missing = [VALUETYPE, CARDINALITY]

    for it in struct[1::]:
      if type(it) == str:
        attrs.append((':db/doc', '"%s"' % it))
      elif 2 == len(it):
        attrs.append(it)
        if it[0] in missing: missing.remove(it[0])
      else:
        enums = it

    if ':db/valueType'   in missing: attrs.append(STRING)
    if ':db/cardinality' in missing: attrs.append(ONE)

    attrs.append((':db.install/_attribute',':db.part/db'))
    self.schema.append("{%s}" % "\n ".join(("%s %s" % (k,v)) for k,v in attrs))

    if enums: 
      for option in enums:
        self.build_enum(ns, struct[0], option)

  def build_enum(self, ns, ident, option):
    if ns:
      st = " [:db/add #db/id[:db.part/user] :db/ident :%s.%s/%s] " % \
             (ns, ident, option)
    else:
      st = " [:db/add #db/id[:db.part/user] :db/ident :%s/%s] " % \
             (ident, option)
    self.schema.append(st)
