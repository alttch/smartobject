from . import config

import importlib
import threading
import logging

from functools import partial

logger = logging.getLogger('smartobject')

storages = {}


def define_storage(storage, id=None):
    """
    Define new object storage

    The storage must implement methods of AbstractStorage class

    Args:
        storage: storage object
        id: storage id, if not specified, default storage is defined
    """
    if id is not None and not isinstance(id, str) and not isinstance(id, int):
        raise ValueError('Storage ID must be string or integer')
    storages[id] = storage


def get_storage(id=None):
    """
    Get object storage

    Args:
        id: storage id, if not specified, default storage is returned
    """
    try:
        return storages[id]
    except KeyError:
        raise RuntimeError(f'Storage "{id}" is not defined')


def purge():
    """
    Purge all deleted objects from all storages

    Returns:
        dict { storage_id: objects_purged }
    """
    return {s: v.purge() for s, v in storages.items()}


class AbstractStorage:
    """
    Abstract storage class which can be used as storage template
    """
    generates_pk = False

    def load(self, pk, **kwargs):
        """
        Load object data from the storage

        Args:
            pk: object primary key
        """
        return {}

    def load_all(self, **kwargs):
        """
        Load object data for all objecta and return it as list generator

        Each object in list is a dictionary with fields:
            data: object data
            info: additional info, e.g. fname the data is loaded from

        info is passed to object after_load() method as kwargs
        """
        while False:
            yield {}

    def save(self, pk, data, modified, **kwargs):
        """
        Save object data to the storage

        Args:
            pk: object primary key
            data: full object data
            modified: modified properties only

        Returns:
            object primary key
        """
        if data or modified:
            raise RuntimeError('Not implemented')

    def delete(self, pk, props, **kwargs):
        """
        Delete object data from the storage

        Args:
            pk: object primary key
            props: list of object properties mapped to store
        """
        raise RuntimeError('Not implemented')

    def get_prop(self, pk, prop, **kwargs):
        """
        Get single object property from the storage

        Must be implemented if external properties are mapped

        Args:
            pk: object primary key
            prop: object property
        """
        raise RuntimeError('Not implemented')

    def set_prop(self, pk, prop, value, **kwargs):
        """
        Save single object property to the storage

        Must be implemented if external properties are mapped

        Args:
            pk: object primary key
            prop: object property
            value: property value
        """
        raise RuntimeError('Not implemented')

    def purge(self, **kwargs):
        """
        Purge deleted objects
        """
        return 0

    def cleanup(self, pks, **kwargs):
        """
        Cleanup storage

        Deletes from storage all items which are not in provided list of
        primary keys

        Args:
            pks: list of primary keys
        Returns:
            number of deleted items
        """
        raise RuntimeError('Not implemented')


class DummyStorage(AbstractStorage):
    """
    Dummy storage class with empty methods

    Does nothing, useful for testing
    """

    def save(self, pk, data, modified, **kwargs):
        return pk

    def delete(self, *args, **kwargs):
        return True


class RedisStorage(AbstractStorage):
    """
    Redis storage

    Implements get_prop/set_prop methods, can be used for external properties

    Can not save/load objects

    Stores object properties in format {pk}/{prop}

    Deletes properties from Redis server when object is deleted
    """

    def __init__(self, host='localhost', port=6379, db=0, prefix='', **kwargs):
        """
        Args:
            host: Redis host
            port: Redis port
            db: Redis DB ID
            prefix: record prefix
            **kwargs: passed to Python redis module as-is
        """
        import redis
        self.r = redis.Redis(host=host, port=port, db=db, **kwargs)
        self.prefix = prefix

    def get_prop(self, pk, prop, **kwargs):
        return self.r.get(f'{self.prefix}{pk}/{prop}')

    def set_prop(self, pk, prop, value, **kwargs):
        self.r.set(f'{self.prefix}{pk}/{prop}', value)

    def delete(self, pk, props, **kwargs):
        self.r.delete(*[f'{self.prefix}{pk}/{prop}' for prop in props])


class SQLAStorage(AbstractStorage):
    """
    RDBMS storage via SQLAlchemy

    Implements get_prop/set_prop methods, can be used for external properties

    Can save/load objects
    
    The table for objects must be created manually before the class methods can
    work
    """
    generates_pk = True

    def __init__(self, db, table, pk_field='id'):
        """
        Args:
            db: either SQLAlchemy instance, which implements "execute" method
                (engine, connection) or callable (function) which returns such
                instance on demand
            table: database table
            pk_field: primary key field in table (default: id)
        """
        self.db = db
        self.table = table
        self.sa = importlib.import_module('sqlalchemy')
        self.pk_field = pk_field
        self.allow_empty = False
        self.__lock = threading.RLock()

    @staticmethod
    def _safe_format(val):
        if val is None: return 'null'
        n_allow = '\'";'
        for al in n_allow:
            if isinstance(val, (list, tuple)):
                val = [
                    v.replace(al, '')
                    if not isinstance(v, (int, float)) and al in v else v
                    for v in val
                ]
            elif isinstance(val, str):
                val = val.replace(al, '') if al in val else val
        return val

    def load(self, pk, **kwargs):
        with self.__lock:
            result = self.get_db().execute(self.sa.text(
                'select * from {table} where {pk_field}=:pk'.format(
                    table=self.table, pk_field=self.pk_field)),
                                           pk=pk).fetchone()
            if result is None:
                if self.allow_empty: return {}
                else: raise LookupError
            else:
                return dict(result)

    def load_all(self, **kwargs):
        with self.__lock:
            result = self.get_db().execute(f'select * from {self.table}')
            while True:
                d = result.fetchone()
                if d is None: break
                yield { 'data':  dict(d) }

    def get_prop(self, pk, prop, **kwargs):
        with self.__lock:
            result = self.get_db().execute(self.sa.text(
                'select {prop} from {table} where {pk_field}=:pk'.format(
                    prop=prop, table=self.table, pk_field=self.pk_field)),
                                           pk=pk).fetchone()
            if result is None:
                raise LookupError(f'Object {pk} not saved yet')
            return result[prop]

    def set_prop(self, pk, prop, value, **kwargs):
        with self.__lock:
            if not self.get_db().execute(self.sa.text(
                    'update {table} set {prop}=:value where {pk_field}=:pk'.
                    format(prop=prop, table=self.table,
                           pk_field=self.pk_field)),
                                         pk=pk,
                                         value=value).rowcount:
                raise LookupError(f'Object {pk} not saved yet')
            return True

    def save(self, pk=None, data={}, modified={}, **kwargs):
        with self.__lock:
            db = self.get_db()
            updates = [
                '{}="{}"'.format(k, self._safe_format(v))
                for k, v in modified.items()
            ]
            if pk is not None:
                result = db.execute(self.sa.text(
                    'update {table} set {update} where {pk_field}=:pk'.format(
                        table=self.table,
                        pk_field=self.pk_field,
                        update=','.join(updates))),
                                    pk=pk)
            if pk is None or not result.rowcount:
                if pk is None:
                    fields = []
                    values = []
                else:
                    fields = [self.pk_field]
                    values = ['"{}"'.format(self._safe_format(pk))]
                for k, v in data.items():
                    fields.append(k)
                    values.append("{}".format(self._safe_format(v)))
                result = db.execute(
                    'insert into {table} ({fields}) values ({values})'.format(
                        table=self.table,
                        fields=','.join(fields),
                        values=','.join(values)))
                if pk is None:
                    pk = result.lastrowid
            return pk

    def cleanup(self, pks, **kwargs):
        c = 0
        todel = []
        with self.__lock:
            db = self.get_db()
            result = db.execute(f'select {self.pk_field} from {self.table}')
            while True:
                d = result.fetchone()
                if not d: break
                pk = getattr(d, self.pk_field)
                if pk not in pks:
                    todel.append(pk)
                    c += 1
            for pk in todel:
                db.execute(self.sa.text(f"""delete from
                            {self.table} where {self.pk_field}=:pk"""),
                           pk=pk)
        return c

    def delete(self, pk, props, **kwargs):
        with self.__lock:
            return self.get_db().execute(self.sa.text(
                'delete from {table} where {pk_field}=:pk'.format(
                    table=self.table, pk_field=self.pk_field)),
                                         pk=pk).rowcount > 0

    def get_db(self):
        return self.db() if callable(self.db) else self.db


class AbstractFileStorage(AbstractStorage):
    """
    Abstract class for file-based storages

    Has the following properties:

        allow_empty: if no object data file is found, return empty data (defalt: True)

        instant_delete: delete object files instantly (default: True)

    File-based storages usually don't implement get_prop/set_prop methods

    File-based storages have additional "fname" property for load() method
    """
    generates_pk = True

    def __init__(self):
        self.dir = None
        self._ext = 'dat'
        self._binary = False
        self.allow_empty = True
        self.instant_delete = True
        self._files_to_delete = set()
        self.__lock = threading.RLock()

    def save(self, pk=None, data={}, modified={}, **kwargs):
        if pk is None:
            import uuid
            pk = str(uuid.uuid4())
        fname = (self.dir if self.dir is not None else config.storage_dir
                ) + '/' + pk.replace('/', '___') + '.' + self._ext
        with self.__lock:
            try:
                self._files_to_delete.remove(fname)
            except KeyError:
                pass
            with open(fname, 'w' + ('b' if self._binary else '')) as fh:
                fh.write(self.dumps(data))
            return pk

    def prepare_pk(self, pk):
        """
        Converts primary key value to file name

        By default, replaces "/" with "___"
        """
        return str(pk).replace('/', '___')

    def load(self, pk=None, fname=None, allow_empty=None, **kwargs):
        if pk is None and fname is None:
            raise ValueError('Either pk or fname must be specified')
        with self.__lock:
            try:
                if fname is None:
                    fname = (self.dir
                             if self.dir is not None else config.storage_dir
                            ) + '/' + self.prepare_pk(pk) + '.' + self._ext
                with open(fname, 'r' + ('b' if self._binary else '')) as fh:
                    return self.loads(fh.read())
            except FileNotFoundError:
                if (self.allow_empty and
                        allow_empty is not False) or allow_empty is True:
                    return {}
                raise

    def list(self, pattern=None, **kwargs):
        """
        List object files in storage

        Args:
            pattern: file pattern (default: all files with {self.ext})
        """
        from pathlib import Path
        return Path(self.dir if self.dir is not None else config.storage_dir
                   ).glob(pattern if pattern is not None else f'*.{self._ext}')

    def load_all(self, pattern=None, **kwargs):
        with self.__lock:
            for f in self.list(pattern=pattern):
                logging.debug(f'Loading object data from {f}')
                yield {
                    'info': {
                        'fname': f
                    },
                    'data': self.load(fname=f, allow_empty=False)
                }

    def cleanup(self, pks, pattern=None):
        ppks = [self.prepare_pk(pk) for pk in pks]
        for f in self.list(pattern=pattern):
            if f.stem not in ppks:
                f.unlink()

    def delete(self, pk, props, **kwargs):
        """
        Delete stored object
        """
        with self.__lock:
            fname = (self.dir if self.dir is not None else config.storage_dir
                    ) + '/' + str(pk).replace('/', '___') + '.' + self._ext
            if self.instant_delete:
                import os
                try:
                    os.unlink(fname)
                    return True
                except:
                    return False
            else:
                self._files_to_delete.add(fname)

    def purge(self, **kwargs):
        import os
        with self.__lock:
            c = 0
            for fname in self._files_to_delete:
                try:
                    os.unlink(fname)
                    c += 1
                except:
                    pass
            self._files_to_delete.clear()
            return c


class JSONStorage(AbstractFileStorage):
    """
    Stores object data in JSON format

    Uses rapidjson module if installed, otherwise fallbacks to default
    """

    def __init__(self, pretty=False):
        """
        Args:
            pretty: if True, store pretty-formatted JSON (indent, sort keys),
                default is False
        """
        super().__init__()
        self._ext = 'json'
        try:
            j = importlib.import_module('rapidjson')
        except:
            j = importlib.import_module('json')
        self.loads = j.loads
        self.dumps = partial(j.dumps, indent=4,
                             sort_keys=True) if pretty else j.dumps


class YAMLStorage(AbstractFileStorage):
    """
    Stores object data in YAML format

    Requires pyyaml module
    """

    def __init__(self, pretty=True):
        """
        Args: pretty: if True, store pretty-formatted YAML, default is True
        """
        super().__init__()
        self._ext = 'yml'
        yaml = importlib.import_module('yaml')
        self.loads = yaml.load
        self.dumps = partial(yaml.dump, default_flow_style=not pretty)


class PickleStorage(AbstractFileStorage):
    """
    Stores object data in Python pickle format
    """

    def __init__(self):
        super().__init__()
        self._ext = 'p'
        self._binary = True
        pickle = importlib.import_module('pickle')
        self.loads = pickle.loads
        self.dumps = pickle.dumps


class MessagePackStorage(AbstractFileStorage):
    """
    Stores object data in MessagePack format

    Requires msgpack-python module
    """

    def __init__(self):
        super().__init__()
        self._ext = 'msgpack'
        self._binary = True
        self.msgpack = importlib.import_module('msgpack')
        self.loads = partial(self.msgpack.loads, raw=False)

    def dumps(self, data):
        return self.msgpack.Packer(use_bin_type=True).pack(data)


class CBORStorage(AbstractFileStorage):
    """
    Stores object data in CBOR format

    Requires cbor module
    """

    def __init__(self):
        super().__init__()
        self._ext = 'cbor'
        self._binary = True
        cbor = importlib.import_module('cbor')
        self.loads = cbor.loads
        self.dumps = cbor.dumps
