SmartObjectFactory
******************

Factory object to create and manipulate SmartObject-based objects.

.. contents::

SmartObjectFactory class
========================

.. automodule:: smartobject.factory

.. autoclass:: SmartObjectFactory
   :inherited-members:
   :members:
   :show-inheritance:

Object auto-loading
===================

By primary key
--------------

If *autoload=True* argument is used on factory creation, the factory will try
automatically load Smart Objects from the storage when **factory.get()** method
is called and the object doesn't exist.

To make this working, the following conditions must be met:

- the object class should accept empty arguments for constructor (primary key
  will be set by factory)

- **factory.get()** method should be called as getting object by primary key.

If *allow_empty=False* for the object storage, **factory.get()** method raises
*LookupError* (for RDBMS storages) or *FileNotFoundError* (for file-based
storages) exceptions in case if storage doesn't have an object with such PK.

By property
-----------

SmartObjectFactory also supports loading objects by property as secondary key.
To use this feature:

* Factory **autoload** option should be set to *True*
* For get method, **prop** argument should be specified
* You may also specify storage ID to load missing objects from, if not
  specified - default storage is used
* Objects are auto-loaded from the storage in two cases:

   * when object is missing in factory
   * when *get_all=True* is specified for **factory.get()** method.

.. note::

   Loading objects by prop is supported only for RDBMS storages

Object cache size limit
=======================

By default, SmartObjectFactory stores all created/appended objects in internal
object list.

If you limit factory object size list with constructor param *maxsize*, it will
start working as LRU cache: when the new object is created or appended, the
factory will delete the least recently accessed objects to make sure the list
size is below or equal to maximum.

Note, that when SmartObjectFactory works as LRU cache, you are unable to use
**factory.cleanup_storage()** method (it will raise *RuntimeError* exception).

Usually, to make object cache work properly, object auto-loading feature should
be also enabled.
