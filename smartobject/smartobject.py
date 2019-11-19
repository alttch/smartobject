from . import config
from . import storage
from . import sync
from . import constants

import logging
import threading

from functools import partial
from itertools import chain

logger = logging.getLogger('smartobject')


def val_to_boolean(s):
    if isinstance(s, bool): return s
    if s is None: return None
    val = str(s)
    if val.lower() in ['1', 'true', 'yes', 'on', 'y']: return True
    if val.lower() in ['0', 'false', 'no', 'off', 'n']: return False
    return None


class SmartObject(object):

    def __getattribute__(self, name):
        if not name.startswith('_') and config.auto_externals:
            try:
                if name in self.__externals:
                    return self._format_value(
                        name,
                        storage.get_storage(self.__externals[name]).get_prop(
                            self._get_primary_key(), name))
            except AttributeError:
                pass
        return super().__getattribute__(name)

    def __setattr__(self, name, value):
        if not name.startswith('_') and config.auto_externals:
            try:
                if name in self.__externals:
                    return storage.get_storage(self.__externals[name]).set_prop(
                        self._get_primary_key(), name, value)
            except AttributeError:
                pass
        return super().__setattr__(name, value)

    def load_property_map(self, property_map=None, override=False):
        if not hasattr(self, '_property_map'):
            self._property_map = {}
        if isinstance(property_map, dict):
            new_property_map = property_map
        else:
            import yaml
            fname = f'{config.property_maps_dir}/{property_map}' if \
                    property_map.find('/') == -1 else property_map
            with open(fname) as fh:
                new_property_map = yaml.load(fh)
        for k, v in new_property_map.items():
            if k not in self._property_map or override:
                self._property_map[k] = v

    def apply_property_map(self):
        self.__serialize_map = {None: []}
        self.__primary_key_field = None
        self.__deleted = False
        self.__modified = {None: set()}
        self.__storages = set()
        self.__storage_map = {}
        self.__modified_for_sync = {None: set()}
        self.__sync_always = {None: set()}
        self.__syncs = set()
        self.__sync_map = {}
        self.__externals = {}
        self._object_factory = None
        self.__snapshot = None
        for i, v in self._property_map.items():
            if v is None:
                v = {}
                self._property_map[i] = {}
            if 'type' in v:
                tp = v['type']
                if tp is not None:
                    v['type'] = eval(tp) if isinstance(tp, str) else tp
            if v.get('pk') is True:
                if self.__primary_key_field is not None:
                    raise RuntimeError('Multiple primary keys defined')
                else:
                    self.__primary_key_field = i
            if not hasattr(self, i) and not v.get('external'):
                setattr(self, i, v.get('default'))
            self.__serialize_map[None].append(i)
            ser = v.get('serialize')
            if 'sync' in v and v['sync'] is not False:
                sync_id = v['sync']
                if sync_id is True:
                    sync_id = None
                    v['sync'] = None
                self.__modified_for_sync.setdefault(sync_id, set()).add(i)
                self.__syncs.add(sync_id)
                if sync_id not in self.__sync_always:
                    self.__sync_always[sync_id] = set()
                if v.get('sync-always'):
                    self.__sync_always[sync_id].add(i)
                else:
                    self.__sync_map.setdefault(sync_id, set()).add(i)
            if 'store' in v and v['store'] is not False:
                storage_id = v['store']
                if storage_id is True:
                    storage_id = None
                    v['store'] = None
                self.__modified.setdefault(storage_id, set()).add(i)
                self.__storages.add(storage_id)
                self.__storage_map.setdefault(storage_id, set()).add(i)
                if v.get('external'):
                    self.__externals[i] = storage_id
            if ser:
                for s in ser if isinstance(ser, list) else [ser]:
                    self.__serialize_map.setdefault(s, []).append(i)
        if self.__primary_key_field is None:
            raise RuntimeError('Primary key is not defined')
        self._get_primary_key = partial(getattr, self, self.__primary_key_field)
        self.__lock = threading.RLock()
        self.__cerr = f'for objects of class "{self.__class__.__name__}"'

    def __check_deleted(self):
        if self.deleted:
            raise RuntimeError('object {c} {pk} is deleted'.format(
                c=self.__class__.__name__, pk=self._get_primary_key()))

    def storage_get(self, prop):
        return self._format_value(
            prop,
            storage.get_storage(self._property_map[prop]['store']).get_prop(
                self._get_primary_key(), prop))

    def storage_set(self, prop, value):
        storage.get_storage(self._property_map[prop]['store']).set_prop(
            self._get_primary_key(), prop, value)

    def _format_value(self, prop, value):
        p = self._property_map[prop]
        if 'type' in p:
            tp = p.get('type')
            try:
                if value is not None and type(value) is not tp:
                    if tp == str:
                        value = value.decode() if isinstance(
                            value, bytes) else str(value)
                    elif tp == bytes:
                        value = str(value).encode()
                    elif tp == bool:
                        value = val_to_boolean(value)
                    elif tp == int or tp == float:
                        try:
                            value = tp(value)
                        except ValueError:
                            if p.get('accept-hex'):
                                value = int(value, 16)
                            else:
                                raise
                    else:
                        raise ValueError
            except ValueError:
                raise ValueError(
                    f'invalid value: {prop}="{value}" {self.__cerr}')
        if 'choices' in p and value not in p.get('choices'):
            raise ValueError(f'invalid value: {prop}="{value}" {self.__cerr}')
        return value

    def set_prop(self,
                 prop=None,
                 value=None,
                 save=False,
                 sync=True,
                 _allow_readonly=False):
        """
        Set object property by prop/value

        To set multiple properties at once, specify value as dict

        Args:
            prop: object property prop
            value: object property value
            save: auto-save object if properties were modified
            sync: sync object if properties were modified

        Returns:
            True if property is set, False if unchanged

        Raises:
            AttributeError: if no such property or property is read-only
            ValueError: if property value is invalid or no prop specified
            TypeError: if object is deleted
        """
        with self.__lock:
            self.__check_deleted()
            if isinstance(prop, dict) and value is None:
                value = prop
                prop = None
            if isinstance(value, dict) and prop is None:
                result = False
                self.snapshot_create()
                try:
                    for i, v in value.items():
                        result = self.set_prop(
                            i,
                            v,
                            save=False,
                            sync=False,
                            _allow_readonly=_allow_readonly) or result
                except:
                    self.snapshot_rollback()
                    raise
                if result is True:
                    if sync:
                        self.sync()
                    if save:
                        self.save()
                return result
            else:
                if prop is None:
                    raise ValueError('prop is not specified')
                if not isinstance(prop, str):
                    raise ValueError('prop should be string')
                p = self._property_map.get(prop)
                if p is None:
                    raise AttributeError(
                        f'no such property: "{prop}" {self.__cerr}')
                if p.get('read-only') and not _allow_readonly:
                    raise AttributeError(
                        f'property "{prop}" is read-only {self.__cerr}')
                if value is None and 'default' in p:
                    value = p['default']
                value = self._format_value(prop, value)
                value = self.prepare_value(prop, value)
                external = p.get('external')
                if external or getattr(self, prop) != value:
                    setattr(self, prop, value)
                    logger.log(
                        p.get('log-level', 20),
                        'Setting {c} {pk} {prop}="{value}"'.format(
                            c=self.__class__.__name__,
                            pk=self._get_primary_key(),
                            prop=prop,
                            value='***' if p.get('log-hide-value') else value))
                    if not external:
                        if 'sync' in p:
                            sync_id = p['sync']
                            if sync: self.sync()
                            self.__modified_for_sync[sync_id].add(prop)
                        if 'store' in p:
                            storage_id = p['store']
                            self.__modified[storage_id].add(prop)
                        if save: self.save()
                    return True and not external
                return False

    def prepare_value(self, key, value):
        return value

    def serialize(self, mode=None, allow__deleted=False):
        if not allow__deleted: self.__check_deleted()
        with self.__lock:
            return {
                key: self.serialize_prop(key)
                for key in self.__serialize_map[mode]
            }

    def serialize_prop(self, prop, target=None):
        """
        Args:
            prop: object property to serialize
            target: smartobject.SERIALIZE_SAVE or SERIALIZE_SYNC
        """
        return getattr(self, f'serialize_{prop}')(target=target) if hasattr(
            self, f'serialize_{prop}') else getattr(self, prop)

    def load(self):
        self.__check_deleted()
        logger.debug('Loading {c} {pk}'.format(c=self.__class__.__name__,
                                               pk=self._get_primary_key()))
        for storage_id in self.__storages:
            self.set_prop(value={
                key: value
                for key, value in storage.get_storage(storage_id).load(
                    pk=self._get_primary_key()).items()
                if not self._property_map[key].get('external')
            },
                          _allow_readonly=True)
            self.__modified[storage_id].clear()

    def sync(self, force=False):
        self.__check_deleted()
        sync_tasks = {}
        with self.__lock:
            pk = self._get_primary_key()
            for sync_id, props in self.__sync_map.items(
            ) if force else self.__modified_for_sync.items():
                sync_data = {
                    key: self.serialize_prop(key,
                                             target=constants.SERIALIZE_SYNC)
                    for key in chain(props, self.__sync_always[sync_id])
                }
                if not force: props.clear()
                sync_tasks[sync_id] = sync_data
        for sync_id, sync_data in sync_tasks.items():
            sync.get_sync(sync_id).sync(pk, sync_data)
        return True

    def save(self, force=False):
        self.__check_deleted()
        pk = self._get_primary_key()
        logger.debug('Saving {c} {pk}'.format(c=self.__class__.__name__, pk=pk))
        with self.__lock:
            for storage_id in self.__modified:
                if self.__modified[storage_id] or force:
                    data = {
                        key:
                        self.serialize_prop(key,
                                            target=constants.SERIALIZE_SAVE)
                        for key, props in self._property_map.items()
                        if 'store' in props and props['store'] == storage_id and
                        not props.get('external')
                    }
                    storage.get_storage(storage_id).save(
                        pk=pk,
                        data=data,
                        modified=data if force else {
                            key: data[key]
                            for key in self.__modified[storage_id]
                            if key in data
                        })
                    self.__modified[storage_id].clear()

    def snapshot_create(self):
        with self.__lock:
            snapshot = {
                key: getattr(self, key)
                for key, prop in self._property_map.items()
                if not prop.get('read-only') and not prop.get('external')
            }
            self.__snapshot = snapshot
            return snapshot.copy()

    def snapshot_rollback(self, snapshot=None):
        with self.__lock:
            if self.__snapshot is None and shapsnot is None:
                raise ValueError('No snapshot created')
            self.set_prop(value=snapshot if snapshot else self.__snapshot)

    def delete(self, _call_factory=True):
        pk = self._get_primary_key()
        logging.warning(self._object_factory)
        if self._object_factory and _call_factory:
            self._object_factory.delete(pk)
        else:
            self.__check_deleted()
            logger.info('Deleting {c} {pk}'.format(c=self.__class__.__name__,
                                                   pk=pk))
            if not self.__deleted:
                with self.__lock:
                    self.__deleted = True
                    for storage_id in self.__storages:
                        storage.get_storage(storage_id).delete(
                            pk, self.__storage_map[storage_id])
                    for sync_id in self.__syncs:
                        sync.get_sync(sync_id).delete(pk)

    @property
    def deleted(self):
        return self.__deleted
