#!/usr/bin/env pytest

from pathlib import Path
import sys
import pytest
import logging

sys.path.insert(0, Path().absolute().parent.as_posix())
import smartobject

smartobject.config.property_maps_dir = 'mappings'
smartobject.config.storage_dir = 'test_data'

smartobject.define_sync(smartobject.DummySync())


class Person(smartobject.SmartObject):

    def __init__(self, name, etest=None):
        self.name = name
        self.load_property_map('person.yml')
        if etest:
            self.load_property_map({
                'heartbeat': {
                    'type': float,
                    'store': etest,
                    'external': True
                }
            })
        self.apply_property_map()


class Employee(Person):

    def __init__(self, name, etest=None):
        self.load_property_map()
        self.load_property_map({
            'salary': {
                'type': float,
                'default': 0,
                'store': True,
                'serialize': 'salary'
            }
        })
        super().__init__(name, etest)

    def serialize_salary(self, target):
        return self.salary * 100

    @property
    def department(self):
        return self._department

    @department.setter
    def department(self, value):
        self._department = value
        self.key = '{}/{}'.format(value, self.name.replace(' ', '_').lower())


def test_create_employee():
    employee = Employee('John Doe')
    employee.salary = 1000
    data = employee.serialize()
    assert data['salary'] == 1000 * 100
    assert data['name'] == 'John Doe'
    assert data['key'] == 'coders/john_doe'
    assert data['can_code'] is None


@pytest.mark.parametrize(('tp'), ('JSONStorage', 'YAMLStorage', 'PickleStorage',
                                  'MessagePackStorage', 'CBORStorage'))
def test_storage(tp):
    employee = Employee('John Doe')
    employee.set_prop('salary', 1500)
    storage = getattr(smartobject, tp)()
    storage.allow_empty = False
    smartobject.define_storage(storage)
    smartobject.define_storage(smartobject.DummyStorage(), 'db1')
    employee.save()
    employee2 = Employee('John Doe')
    employee2.load()
    assert employee2.salary == 1500 * 100
    employee2.delete()
    employee3 = Employee('John Doe')
    with pytest.raises(FileNotFoundError):
        employee3.load()
    storage.allow_empty = True


def test_db_storage():
    smartobject.define_storage(smartobject.JSONStorage())
    import sqlalchemy as sa
    import os
    try:
        os.unlink('test_data/test.db')
    except:
        pass
    db = sa.create_engine('sqlite:///test_data/test.db')

    def get_connection():
        return db.connect()

    get_connection().execute("""
    create table pr (
        name varchar(30) not null,
        projects_created int not null,
        heartbeat int,
        primary key(name)
        )
    """)

    storage = smartobject.SQLAStorage(get_connection, 'pr', 'name')
    storage.allow_empty = False
    smartobject.define_storage(storage, 'db1')

    employee = Employee('John Doe', etest='db1')
    with pytest.raises(LookupError):
        employee.load()
    storage.allow_empty = True
    employee.load()
    employee.save(force=True)
    employee.heartbeat = 150
    assert employee.heartbeat == 150
    employee.set_prop('projects_created', 10, save=True)
    employee2 = Employee('John Doe', etest='db1')
    employee2.load()
    assert employee2.projects_created == 10
    employee.set_prop('projects_created', 15)
    employee.save()
    employee2.load()
    assert employee2.heartbeat == 150
    assert employee2.projects_created == 15
    employee2.delete()
    storage.purge()


def test_redis_storage():
    smartobject.define_storage(smartobject.RedisStorage(), 'r1')
    employee = Employee('John Doe', etest='r1')
    employee.save()
    employee.set_prop('heartbeat', '150')
    employee2 = Employee('John Doe', etest='r1')
    employee2.load()
    assert employee2.heartbeat == 150
    employee2.delete()


def test_postpone_delete():
    storage = smartobject.JSONStorage()
    storage.instant_delete = False
    storage.allow_empty = False
    smartobject.define_storage(storage)
    smartobject.define_storage(smartobject.DummyStorage(), 'db1')
    employee = Employee('John Doe')
    employee.save()
    employee.delete()
    employee2 = Employee('John Doe')
    employee2.load()
    employee2.delete()
    smartobject.purge()
    employee3 = Employee('John Doe')
    with pytest.raises(FileNotFoundError):
        employee3.load()
    storage.allow_empty = True


def test_hex():
    employee = Employee('John Doe')
    employee.set_prop('personal_code', '0xFF')
    assert employee.personal_code == 255


def test_invalid_property():
    employee = Employee('John Doe')
    with pytest.raises(AttributeError):
        employee.set_prop('something', 999)


def test_invalid_value():
    employee = Employee('John Doe')
    with pytest.raises(ValueError):
        employee.set_prop('salary', 'xxx')


def test_read_only():
    employee = Employee('John Doe')
    with pytest.raises(AttributeError):
        employee.set_prop('key', 'xxx')


def test_set_default():
    employee = Employee('John Doe')
    employee.set_prop('salary')
    assert employee.salary == 0


def test_set_bool():
    employee = Employee('John Doe')
    employee.set_prop('can_code', 1)
    assert employee.can_code is True


def test_set_warn():
    employee = Employee('John Doe')
    employee.set_prop('password', '123')


def test_set_batch():
    employee = Employee('John Doe')
    employee.set_prop(value={'can_code': 0, 'salary': 100})
    assert employee.salary == 100
    assert employee.can_code is False


def test_set_deleted():
    employee = Employee('John Doe')
    employee.delete()
    with pytest.raises(RuntimeError):
        employee.set_prop('salary', 100)


def test_partial_seralize():
    employee = Employee('John Doe')
    s = employee.serialize('salary')
    assert len(s) == 2
    assert s['name'] == 'John Doe'
    assert s['salary'] == 0
    s = employee.serialize('info')
    assert len(s) == 2
    assert s['name'] == 'John Doe'
    assert s['department'] == 'coders'


def test_invalid_choice():
    employee = Employee('John Doe')
    employee.set_prop('sex', 'other')
    employee.set_prop('sex', 'female')
    employee.set_prop('sex', 'male')
    with pytest.raises(ValueError):
        employee.set_prop('sex', 'alien')


def test_sync():

    class TestSync(smartobject.AbstractSync):

        def __init__(self):
            super().__init__()
            self.sync_counter = 0

        def sync(self, pk, data, **kwargs):
            assert pk == 'coders/john_doe'
            assert data['personal_code'] == 99
            assert data['projects_created'] == 2
            assert self.sync_counter <= 1
            self.sync_counter += 1

        def delete(self, pk):
            assert pk == 'coders/john_doe'

    smartobject.define_sync(TestSync())

    employee = Employee('John Doe')
    employee.set_prop({'personal_code': 99, 'projects_created': 2})
    employee.delete()

    smartobject.define_sync(smartobject.DummySync())


def test_factory():
    smartobject.define_storage(smartobject.JSONStorage())
    smartobject.define_storage(smartobject.DummyStorage(), 'db1')
    factory = smartobject.SmartObjectFactory(Employee)
    employee = factory.create(name='John Doe')
    pk = employee.key
    factory.set_prop(pk, {'personal_code': '0xFF'})
    factory.save()
    factory.save(pk, force=True)
    with pytest.raises(RuntimeError):
        employee = factory.create(name='John Doe')
    employee = factory.create(name='John Doe', override=True, load=False)
    assert employee.personal_code is None
    factory.load(pk)
    assert employee.personal_code == 255
    factory.delete(pk)
    employee = factory.create(name='John Doe', override=True, load=False)
    employee.delete()
    with pytest.raises(KeyError):
        factory.get(employee.key)


def test_snapshots():
    employee = Employee('John Doe')
    employee.set_prop('salary', 100)
    assert employee.salary == 100
    snapshot = employee.snapshot_create()
    assert employee.salary == 100
    employee.set_prop('salary', 150)
    employee.snapshot_rollback()
    assert employee.salary == 100
    employee.set_prop('salary', 150)
    employee.snapshot_create()
    employee.snapshot_rollback(snapshot)
    assert employee.salary == 100
    with pytest.raises(ValueError):
        employee.set_prop({'salary': 150, 'personal_code': 'x'})
    assert employee.salary == 100


import os
os.system('mkdir -p test_data && rm -rf test_data/*')

test_factory()
