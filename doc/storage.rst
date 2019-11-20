Storage
*******

Object properties data is stored into storages. You may define one or multiple
storages, each property can be :doc:`mapped <map>` only to the single storage.

E.g. you may store part of the object data into files and some values in RDBMS
database.

You may use ready-made storage classes or create your own.

.. contents::

Defining storages
=================

.. code:: python

   import smartobject

   # define default storage
   smartobject.define_storage(smartobject.JSONStorage())

   # define another file storage with id "stor2"
   storage2 = smartobject.MessagePackStorage()
   # raise errors if data file is not found
   storage2.allow_empty = False
   smartobject.define_storage(storage2, 'stor2')

   # define key-value storage with id "redis1"
   smartobject.define_storage(smartobject.RedisStorage(), 'redis1')

   # get storage object
   r = smartobject.get_storage('redis1')

   # purge deleted objects in all storages
   smartobject.purge()

.. automodule:: smartobject.storage

File-based storage
==================

File-based storages can not handle properties, marked as "external".

JSON
----

.. autoclass:: JSONStorage
   :members:
   :inherited-members:
   :show-inheritance:

YAML
----

.. autoclass:: YAMLStorage
   :members:
   :inherited-members:
   :show-inheritance:

Pickle
------

.. autoclass:: PickleStorage
   :members:
   :inherited-members:
   :show-inheritance:

MessagePack
-----------

.. autoclass:: MessagePackStorage
   :members:
   :inherited-members:
   :show-inheritance:

CBOR
----

.. autoclass:: CBORStorage
   :members:
   :inherited-members:
   :show-inheritance:

Database storages
=================

RDBMS
-----

`SQLAlchemy <https://www.sqlalchemy.org/>`_-based storage.

.. autoclass:: SQLAStorage
   :members:
   :inherited-members:
   :show-inheritance:

Key-value
---------

`Redis <https://redis.io/>`_-based storage.

.. autoclass:: RedisStorage
   :members:
   :inherited-members:
   :show-inheritance:

Custom storages
===============

You may use the following classes as prototypes for the own storages:

.. autoclass:: AbstractStorage
   :members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: AbstractFileStorage
   :members:
   :show-inheritance:
