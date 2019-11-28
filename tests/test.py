#!/usr/bin/env pytest

from pathlib import Path
import sys
import pytest
import logging

sys.path.insert(0, Path().absolute().parent.as_posix())
import smartobject
import sqlalchemy as sa

smartobject.config.property_maps_dir = 'map'
smartobject.config.storage_dir = 'test_data'

smartobject.define_sync(smartobject.DummySync())


class T2(smartobject.SmartObject):

    def __init__(self, id=None):
        self.id = id
        self.load_property_map()
        self.apply_property_map()

    def after_load(self, opts, **kwargs):
        if 'fname' in opts:
            self.id = opts['fname'].stem


class Person(smartobject.SmartObject):

    def __init__(self, name, etest=None):
        self.name = self.prepare_value('name', name)
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

    def prepare_value(self, prop, value):
        if prop == 'name':
            if value is not None and value.find('/') != -1:
                raise ValueError(f'name contains invalid characters: {value}')
        return value


class Employee(Person):

    def __init__(self, name=None, etest=None):
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
        self.update_pk()

    def update_pk(self):
        if self.name is not None:
            self.id = '{}/{}'.format(self._department,
                                     self.name.replace(' ', '_').lower())

    def after_load(self, opts, **kwargs):
        self.update_pk()


def test_create_employee():
    employee = Employee('John Doe')
    employee.salary = 1000
    data = employee.serialize()
    assert data['salary'] == 1000 * 100
    assert data['name'] == 'John Doe'
    assert data['id'] == 'coders/john_doe'
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
    clean()
    db = sa.create_engine('sqlite:///test_data/test.db')

    def get_connection():
        return db.connect()

    get_connection().execute("""
    create table pr (
        id varchar(30) not null,
        projects_created int not null,
        heartbeat int,
        primary key(id)
        )
    """)

    storage = smartobject.SQLAStorage(get_connection, 'pr', 'id')
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


def __xtest_set_min_max():
    employee = Employee('John Doe')
    with pytest.raises(ValueError):
        employee.set_prop('projects_created', -1)
    with pytest.raises(ValueError):
        employee.set_prop('projects_created', 999999)
    with pytest.raises(ValueError):
        employee.set_prop('password', '1')
    with pytest.raises(ValueError):
        employee.set_prop('password', '1234567890123456890')


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
    employee = factory.create(opts={'name': 'John Doe'})
    pk = employee.id
    factory.set_prop(pk, {'personal_code': '0xFF'})
    factory.save()
    factory.save(pk, force=True)
    factory.serialize(pk)
    s = employee.serialize('salary')
    assert len(s) == 2
    with pytest.raises(RuntimeError):
        employee = factory.create({'name': 'John Doe'})
    employee = factory.create({'name': 'John Doe'}, override=True, load=False)
    assert employee.personal_code is None
    factory.load(pk)
    assert employee.personal_code == 255
    factory.delete(pk)
    employee = factory.create({'name': 'John Doe'}, override=True, load=False)
    employee.delete()
    with pytest.raises(KeyError):
        factory.get(employee.id)
    employee = Employee(name='Jane Doe')
    factory.append(employee)


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


def test_apply_property_map_twice():
    employee = Employee('John Doe')
    with pytest.raises(RuntimeError):
        employee.apply_property_map()


def test_alive_deleted():
    employee = Employee('John Doe')
    assert employee.alive is True
    assert employee.deleted is False
    employee.delete()
    assert employee.alive is False
    assert employee.deleted is True


def test_load_from_dir():
    clean()
    smartobject.define_storage(smartobject.JSONStorage())
    smartobject.define_storage(smartobject.DummyStorage(), 'db1')
    factory = smartobject.SmartObjectFactory(Employee)
    factory.create(opts={'name': 'John Doe'})
    factory.create(opts={'name': 'Jane Doe'})
    e = factory.create(opts={'name': 'Jack Daniels'})
    e.set_prop('salary', 150)
    key = e.id
    factory.save()
    factory.clear()
    with pytest.raises(KeyError):
        factory.get(key)
    factory.load_all()
    assert len(factory.get()) == 3
    assert factory.get(key).salary == 150 * 100


def test_t2_save_to_file():
    clean()
    smartobject.define_storage(smartobject.JSONStorage())
    factory = smartobject.SmartObjectFactory(T2, autosave=True)
    i = factory.create().id
    assert i is not None
    factory.clear()
    factory.load_all()
    factory.get(i)


def _prepare_t2_db():
    db = sa.create_engine('sqlite:///test_data/t2.db')
    db.execute("""
    create table t2 (
        id integer primary key autoincrement,
        login varchar(30),
        password varchar(30)
        )
    """)
    return db


def test_t2_save_to_db():
    clean()
    db = _prepare_t2_db()
    storage = smartobject.SQLAStorage(db, 't2')
    smartobject.define_storage(storage)
    factory = smartobject.SmartObjectFactory(T2, autosave=True)
    obj = factory.create()
    obj.set_prop({'login': 'test', 'password': '123'}, save=True)
    i = obj.id
    assert i is not None
    factory.clear()
    factory.create(opts={'id': i}, load=True)
    obj = factory.get(i)
    assert obj.login == 'test'
    assert obj.password == '123'


def test_t2_load_from_db():
    clean()
    db = _prepare_t2_db()
    storage = smartobject.SQLAStorage(db, 't2')
    smartobject.define_storage(storage)
    factory = smartobject.SmartObjectFactory(T2, autosave=True)
    ids = []
    for i in range(3):
        obj = factory.create()
        obj.set_prop({'login': f'test{obj.id}', 'password': '123'}, save=True)
        ids.append(obj.id)
    assert len(ids) == 3
    factory.clear()
    factory.load_all()
    for i in ids:
        o = factory.get(i)
        assert o.password == '123'
        assert o.login == f'test{o.id}'


def clean():
    import os
    os.system('mkdir -p test_data && rm -rf test_data/*')


def test_file_cleanup():
    clean()
    smartobject.define_storage(smartobject.JSONStorage())
    smartobject.define_storage(smartobject.DummyStorage(), 'db1')
    factory = smartobject.SmartObjectFactory(Employee)
    names = ['Mike', 'Betty', 'Kate', 'John', 'Boris', 'Ivan']
    for n in names:
        factory.create(opts={'name': n})
    factory.save()
    for n in names:
        p = Path(f'test_data/coders___{n.lower()}.json')
        assert p.exists() and p.is_file()
    factory.clear()
    factory.load_all()
    factory.get('coders/mike')
    factory.get('coders/betty')
    factory.remove('coders/mike')
    factory.remove('coders/betty')
    with pytest.raises(KeyError):
        factory.get('coders/mike')
        factory.get('coders/betty')
    factory.cleanup_storage()
    factory.clear()
    factory.load_all()
    with pytest.raises(KeyError):
        factory.get('coders/mike')
        factory.get('coders/betty')
    factory.clear()


def test_db_cleanup():
    clean()
    db = _prepare_t2_db()
    storage = smartobject.SQLAStorage(db, 't2')
    smartobject.define_storage(storage)
    factory = smartobject.SmartObjectFactory(T2, autosave=True)
    o1 = factory.create()
    o2 = factory.create()
    o3 = factory.create()
    o4 = factory.create()
    o1.load()
    o2.load()
    o3.load()
    o4.load()
    factory.remove(o3.id)
    o3.load()
    factory.cleanup_storage()
    o1.load()
    o2.load()
    with pytest.raises(LookupError):
        o3.load()
    o4.load()


def test_factory_indexes():
    clean()
    smartobject.define_storage(smartobject.JSONStorage())
    factory = smartobject.SmartObjectFactory(T2, autosave=True)
    factory.add_index('login')
    o1 = factory.create()
    o2 = factory.create()
    o3 = factory.create()
    o1.set_prop('login', 'test')
    o2.set_prop('login', 'test2')
    o3.set_prop('login', 'test')
    factory.reindex(o1.id)
    factory.reindex(o2.id)
    factory.reindex(o3.id)
    factory.reindex(o3.id)
    assert len(factory.get('test', prop='login')) == 2
    factory.remove(o1)
    assert len(factory.get('test', prop='login')) == 1
    factory.remove(o3)
    assert len(factory.get('test', prop='login')) == 0


def test_factory_autoload():
    clean()
    storage = smartobject.JSONStorage()
    storage.allow_empty = False
    smartobject.define_storage(storage)
    factory = smartobject.SmartObjectFactory(T2, autoload=True, autosave=True)
    o1 = factory.create()
    o2 = factory.create()
    o3 = factory.create()
    o1.set_prop('login', 'test1')
    o2.set_prop('login', 'test2')
    o3.set_prop('login', 'test3')
    factory.save()
    pk1 = o1.id
    pk2 = o2.id
    pk3 = o3.id
    factory.clear()
    import os
    os.unlink(f'test_data/{pk3}.json')
    assert factory.get(pk1).serialize()['login'] == 'test1'
    assert factory.get(pk2).serialize()['login'] == 'test2'
    with pytest.raises(FileNotFoundError):
        assert factory.get(pk3)


def test_factory_lru():
    clean()
    smartobject.define_storage(smartobject.JSONStorage())
    factory = smartobject.SmartObjectFactory(T2,
                                             autoload=True,
                                             autosave=True,
                                             maxsize=2)
    o1 = factory.create()
    o2 = factory.create()
    o3 = factory.create()
    assert len(factory._objects) == 2
    factory.get(o3.id)
    assert len(factory._objects) == 2
    factory.get(o2.id)
    assert len(factory._objects) == 2
    factory.get(o1.id)
    assert len(factory._objects) == 2
    with pytest.raises(RuntimeError):
        factory.cleanup_storage()


clean()
