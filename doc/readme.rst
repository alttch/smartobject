SmartObject
===========

`SmartObject <https://github.com/alttch/smartobject>`__ is a tool to
easily manipulate object attributes with the API commands, store and
synchronize object data.

Mapped object attributes (called properties) can be automatically
validated, saved, loaded, serialized and synchronized with the external
services.

SmartObject looks like ORM, but it’s different from ORM: object
properties can be stored in storages of different type and combined
together into a single data object.

Property values are automatically processed, validated and synchronized
with external services if required.

Example:
--------

You have a team of people with heart-rate sensors. Data for each person
is stored into local JSON files, heartbeat is stored in Redis database.
How to implement this with SmartObject? Just a few lines of code:

.. code:: python

   import smartobject

   class Person(smartobject.SmartObject):

       def __init__(self, name):
           self.name = name
           self.load_property_map('person.yml')
           self.apply_property_map()

   smartobject.define_storage(smartobject.JSONStorage())
   smartobject.define_storage(smartobject.RedisStorage(), 'r1')

   people = smartobject.SmartObjectFactory(Person)

   people.create(name='John')
   people.create(name='Jane')
   people.create(name='Jack')

   people.set_prop('John', 'sex', 'male')
   people.set_prop('Jane', 'sex', 'female')
   people.set_prop('Jack', { 'sex', 'male' })

   people.save()

   print('Heartbeat of Jack is: {}'.format(people.get('Jack').heartbeat)

The file *person.yml* is a property map for the *Person* object. It can
be loaded from the external YAML file or specified directly, as Python
dict.

The map for the above example looks like:

.. code:: yaml

   name:
       pk: true
   sex:
       type: str
       choices:
           - male
           - female
           - other
       store: true
   heartbeat:
       external: true
       store: r1

Pretty simple, isn’t it? You define a map, SmartObject does the job!

Documentation
-------------

Full documentation is available at https://smartobject.readthedocs.io/
