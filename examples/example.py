import sys
sys.path.insert(0, '..')
import smartobject


class Person(smartobject.SmartObject):

    def __init__(self, name):
        self.name = name
        self.load_property_map('person.yml')
        self.apply_property_map()


smartobject.config.storage_dir = 'data'

smartobject.define_storage(smartobject.JSONStorage())
smartobject.define_storage(smartobject.RedisStorage(), 'r1')

people = smartobject.SmartObjectFactory(Person)

# create objects with factory
people.create(name='John')
people.create(name='Jane')

# create object manually
jack = Person('Jack')

# you can set a single prop
people.set_prop('John', 'sex', 'male')
people.set_prop('Jane', 'sex', 'female')
# or multiple props with dict
# heartbeat value is automatically written to Redis
jack.set_prop({'sex': 'male', 'heartbeat': 100})

# print object info (name and sex only)
from pprint import pprint
pprint(people.serialize('Jane', mode='info'))

people.save()
jack.save()

# clear Jack's sex
jack.set_prop('sex', None)
# load it back
jack.load()

# add Jack to factory
people.create(obj=jack)

# heartbeat value is automatically read from Redis
print('Heartbeat of Jack is: {}'.format(people.get('Jack').heartbeat))
