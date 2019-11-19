from . import config

import importlib
import threading

from functools import partial

storages = {}


def define_storage(storage, id=None):
    if id is not None and not isinstance(id, str) and not isinstance(id, int):
        raise ValueError('Storage ID must be string or integer')
    storages[id] = storage


def get_storage(id=None):
    try:
        return storages[id]
    except KeyError:
        raise RuntimeError(f'Storage "{id}" is not defined')


def purge():
    return {s: v.purge() for s, v in storages.items()}


class AbstractStorage:

    def load(self, *args, **kwargs):
        return {}

    def save(self, pk, data, modified, **kwargs):
        if data or modified:
            raise RuntimeError('Not implemented')

    def delete(self, *args, **kwargs):
        raise RuntimeError('Not implemented')

    def get_prop(self, pk, prop, **kwargs):
        raise RuntimeError('Not implemented')

    def set_prop(self, pk, prop, value, **kwargs):
        raise RuntimeError('Not implemented')

    def purge(self, **kwargs):
        return 0


class DummyStorage(AbstractStorage):

    def save(self, *args, **kwargs):
        return True

    def delete(self, *args, **kwargs):
        return True


class RedisStorage(AbstractStorage):

    def __init__(self, host='localhost', port=6379, db=0, **kwargs):
        import redis
        self.r = redis.Redis(host=host, port=port, db=db, **kwargs)

    def get_prop(self, pk, prop, **kwargs):
        return self.r.get(f'{pk}/{prop}')

    def set_prop(self, pk, prop, value, **kwargs):
        self.r.set(f'{pk}/{prop}', value)

    def delete(self, pk, props, **kwargs):
        self.r.delete(*[f'{pk}/{prop}' for prop in props])


class SQLAStorage(AbstractStorage):

    def __init__(self, db, table, pk_field='id'):
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

    def save(self, pk, data={}, modified={}, **kwargs):
        with self.__lock:
            db = self.get_db()
            updates = [
                '{}="{}"'.format(k, self._safe_format(v))
                for k, v in modified.items()
            ]
            result = db.execute(self.sa.text(
                'update {table} set {update} where {pk_field}=:pk'.format(
                    table=self.table,
                    pk_field=self.pk_field,
                    update=','.join(updates))),
                                pk=pk)
            if not result.rowcount:
                fields = [self.pk_field]
                values = ['"{}"'.format(self._safe_format(pk))]
                for k, v in data.items():
                    fields.append(k)
                    values.append("{}".format(self._safe_format(v)))
                db.execute(
                    'insert into {table} ({fields}) values ({values})'.format(
                        table=self.table,
                        fields=','.join(fields),
                        values=','.join(values)))
            return True

    def delete(self, pk, props, **kwargs):
        with self.__lock:
            return self.get_db().execute(self.sa.text(
                'delete from {table} where {pk_field}=:pk'.format(
                    table=self.table, pk_field=self.pk_field)),
                                         pk=pk).rowcount > 0

    def get_db(self):
        return self.db() if callable(self.db) else self.db


class AbstractFileStorage(AbstractStorage):

    def __init__(self):
        self.dir = None
        self.ext = 'dat'
        self.binary = False
        self.allow_empty = True
        self.instant_delete = True
        self.files_to_delete = set()
        self.__lock = threading.RLock()

    def save(self, pk, data={}, modified={}, **kwargs):
        fname = (self.dir if self.dir is not None else config.storage_dir
                ) + '/' + pk.replace('/', '___') + '.' + self.ext
        with self.__lock:
            try:
                self.files_to_delete.remove(fname)
            except KeyError:
                pass
            with open(fname, 'w' + ('b' if self.binary else '')) as fh:
                fh.write(self.dumps(data))
            return True

    def load(self, pk, **kwargs):
        with self.__lock:
            try:
                with open(
                    (self.dir if self.dir is not None else config.storage_dir) +
                        '/' + pk.replace('/', '___') + '.' + self.ext,
                        'r' + ('b' if self.binary else '')) as fh:
                    return self.loads(fh.read())
            except FileNotFoundError:
                if self.allow_empty: return {}
                raise

    def delete(self, pk, props, **kwargs):
        with self.__lock:
            fname = (self.dir if self.dir is not None else config.storage_dir
                    ) + '/' + pk.replace('/', '___') + '.' + self.ext
            if self.instant_delete:
                import os
                try:
                    os.unlink(fname)
                    return True
                except:
                    return False
            else:
                self.files_to_delete.add(fname)

    def purge(self, **kwargs):
        import os
        with self.__lock:
            c = 0
            for fname in self.files_to_delete:
                try:
                    os.unlink(fname)
                    c += 1
                except:
                    pass
            self.files_to_delete.clear()
            return c


class JSONStorage(AbstractFileStorage):

    def __init__(self, pretty=False):
        super().__init__()
        self.ext = 'json'
        try:
            j = importlib.import_module('rapidjson')
        except:
            j = importlib.import_module('json')
        self.loads = j.loads
        self.dumps = partial(j.dumps, indent=4,
                             sort_keys=True) if pretty else j.dumps


class YAMLStorage(AbstractFileStorage):

    def __init__(self, pretty=False):
        super().__init__()
        self.ext = 'yml'
        yaml = importlib.import_module('yaml')
        self.loads = yaml.load
        self.dumps = partial(yaml.dump, default_flow_style=not pretty)


class PickleStorage(AbstractFileStorage):

    def __init__(self):
        super().__init__()
        self.ext = 'p'
        self.binary = True
        pickle = importlib.import_module('pickle')
        self.loads = pickle.loads
        self.dumps = pickle.dumps


class MessagePackStorage(AbstractFileStorage):

    def __init__(self):
        super().__init__()
        self.ext = 'msgpack'
        self.binary = True
        self.msgpack = importlib.import_module('msgpack')
        self.loads = partial(self.msgpack.loads, raw=False)

    def dumps(self, data):
        return self.msgpack.Packer(use_bin_type=True).pack(data)


class CBORStorage(AbstractFileStorage):

    def __init__(self, pretty=False):
        super().__init__()
        self.ext = 'cbor'
        self.binary = True
        cbor = importlib.import_module('cbor')
        self.loads = cbor.loads
        self.dumps = cbor.dumps
