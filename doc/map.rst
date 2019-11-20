Object property map
***********************

Property mapping should be specified either as YAML file or as Python dict.
Multiple property maps can be loaded.

The loaded maps should be applied before the object methods can work. The maps
can be applied only once.

.. contents::

Loading and applying map
========================

Example:

.. code:: python

   from smartobject import SmartObject

   class MyObject(SmartObject):

      def __init__(self):
         # tries to load map from
         # {smartobject.config.property_maps_dir}/{self.__class__.__name__}
         self.load_property_map()
         # tries to load map from the specified file
         # if no directory specified, smartobject.config.property_maps_dir is
         # used
         self.load_property_map('my.yml')
         # load property map from Python dict
         self.load_property_map({
            'prop1': {
               'type': str,
               'store': True
               }
         })
         self.apply_property_map()

Property map structure
======================

A property map looks like

.. literalinclude:: ../tests/map/person.yml
    :language: yaml

Object primary key
==================

Each SmartObject should have one (and no more than one) property, which is used
as object primary key.

Primary key is used to load/save object, store it in :doc:`object factories
<factory>` etc.

.. code:: yaml

   mymainprop:
      pk: true

Primary key property is automatically marked as read-only and can not be
changed with *set_prop()* method.

Property type and value
=======================

Type
----

Property type may be any valid Python type. The type is automatically validated
by *self.set_prop()* method. If new property value type doesn't match,
SmartObject tries to convert it to the required type (strings to numbers, bytes
to strings, numbers to booleans etc.).

.. code:: yaml

   myprop1:
      type: str

If type is not specified, property can have values of any type.

Choices
-------

Property value can be limited to the list of choices:

.. code:: yaml

   myprop1:
      type: str
      choices:
         - value1
         - value2
         - value3

If property value can be null (None), add *null* value to the choice list.

Default value
-------------

You may specify default property value:

.. code:: yaml

   myprop1:
      type: str
      default: "I am default"

Read-only
---------

Read-only properties can not be modified by *set_prop()* method:

.. code:: yaml

   myprop1:
      read-only: true

Accept hexadecimal values for numbers
-------------------------------------

You may enable hexadecimal values for numbers with:

.. code:: yaml

   myprop1:
      type: int
      accept-hex: true

If hexadecimal string is specified as property value in *set_prop()* method,
it's automatically converted to integer.

Saving and loading properties
=============================

When object is :doc:`loaded or saved <storage>`, only properties mapped to the
storages are processed.

Map property to default object storage:

.. code:: yaml

   myprop1:
      store: true

Map property to object storage with id *stor1*:

.. code:: yaml

   myprop1:
      store: stor1

Synchronization
===============

While **smartobject.save()** method may be deferred,
:doc:`**SmartObject.sync()** <sync>` is called any time when object property is
modified.

To include object property in default synchronization, use:

.. code:: yaml

   myprop1:
      sync: true

To include property in synchronization with id *sync1*:

.. code:: yaml

   myprop1:
      sync: sync1

To always include property into synchronization blocks, use:

.. code:: yaml

   myprop1:
      sync: sync1
      sync-always: true

Serialization
=============

By default, method *SmartObject.serialize()* returns dict with values of all
mapped object properties.

You may specify custom serialization modes and include only required object
properties:

.. code:: yaml

   myprop1:
      serialize: info
   myprop2:
      serialize:
         - info
         - mydata

External properties
===================

If property is marked as external, its value is always being get / set from the
external storage. E.g. you may store dynamic object data in key-value or RDBMS
database.

.. code:: yaml

   myprop1:
      store: db1
      external: true

If *auto_externals* is set to False in :doc:`SmartObject configuration
<config>`, you must create getter and setter for such property manually,
otherwise everything's handled automatically by SmartObject class.

Logging
=======

SmartObject uses logger called "smartobject". All property changes are logged
with INFO level, but you can change it.

Example: log property changes with DEBUG (=10) level:

.. code:: yaml

   myprop1:
      log-level: 30

Example: hide property values for the sensitive fields:

.. code:: yaml

   mysecretprop1:
      log-hide-value: true
