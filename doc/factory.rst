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

If *autoload=True* argument is used on factory creation, the factory will try
automatically load Smart Objects from the storage when **factory.get()** method
is called and the object doesn't exist.

To make this working, the following conditions must be met:

- the object class should accept empty arguments for constructor (primary key
  will be set by factory)

- **factory.get()** method should be called as getting object by primary key.

If **allow_empty=False** for the object storage, **factory.get()** method
raises *LookupError* (for RDBMS storages) or *FileNotFoundError* (for
file-based storages) exceptions in case if storage doesn't have an object with
such PK.
