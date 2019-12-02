from . import config
from . import storage
from . import sync
from . import constants

import logging
import threading

from functools import partial
from itertools import chain

from jsonschema import validate

from pyaltt2.converters import val_to_boolean

logger = logging.getLogger('smartobject')


class SmartObject(object):
    """
    Smart Object implementation class
    """

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
        """
        Load Smart Object property map

        Args:
            property_map: property map to load. Can be dict (used as-is), file
                path or empty (class name + .yml is used for the file name)
            override:
        """
        if not hasattr(self, '_property_map'):
            self._property_map = {}
        if isinstance(property_map, dict):
            new_property_map = property_map
        else:
            import yaml
            if property_map is None:
                property_map = config.property_maps_dir + \
                        f'/{self.__class__.__name__}.yml'
            elif property_map.find('/') == -1:
                property_map = f'{config.property_maps_dir}/{property_map}'
            with open(property_map) as fh:
                new_property_map = yaml.load(fh)
        validate(instance=new_property_map,
                 schema=constants.PROPERTY_MAP_SCHEMA)
        for k, v in new_property_map.items():
            if k not in self._property_map or override:
                self._property_map[k] = v

    def apply_property_map(self):
        """
        Apply loaded property map

        Can be called only once, otherwise raises RuntimeError
        """
        try:
            if self.__property_map_applied:
                raise RuntimeError('Property map is already applied')
        except AttributeError:
            pass
        self.__serialize_map = {None: set()}
        self.__primary_key_field = None
        self.__deleted = False
        self.__modified = {None: set()}
        self.__storages = []
        self.__storage_map = {}
        self.__modified_for_sync = {None: set()}
        self.__sync_always = {None: set()}
        self.__syncs = set()
        self.__sync_map = {}
        self.__externals = {}
        self._object_factory = None
        self.__snapshot = None
        self.__property_map_applied = True
        for i, v in self._property_map.items():
            if v is None:
                v = {}
                self._property_map[i] = {}
            if 'type' in v:
                tp = v['type']
                if tp is not None:
                    v['type'] = eval(tp) if isinstance(tp, str) else tp
            if v.get('pk'):
                if self.__primary_key_field is not None:
                    raise RuntimeError('Multiple primary keys defined')
                else:
                    self.__primary_key_field = i
                v['read-only'] = True
            if not hasattr(self, i) and not v.get('external'):
                setattr(self, i, v.get('default'))
            self.__serialize_map[None].add(i)
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
                # make sure pk storage is first to let it generate pk if
                # doesn't exists
                if v.get('pk'):
                    if storage_id in self.__storages and self.__storages[
                            0] != storage_id:
                        self.__storages.remove(storage_id)
                        self.__storages.insert(0, storage_id)
                elif storage_id not in self.__storages:
                    self.__storages.append(storage_id)
                self.__storage_map.setdefault(storage_id, set()).add(i)
                if v.get('external'):
                    self.__externals[i] = storage_id
            if ser:
                for s in ser if isinstance(ser, list) else [ser]:
                    self.__serialize_map.setdefault(s, set()).add(i)
        if self.__primary_key_field is None:
            raise RuntimeError('Primary key is not defined')
        self.__lock = threading.RLock()
        self.__cerr = f'for objects of class "{self.__class__.__name__}"'

    def _get_primary_key(self, _allow_null=True):
        pk = getattr(self, self.__primary_key_field, None)
        if pk is None and not _allow_null:
            raise ValueError('Primary key is not set')
        return pk

    def _set_primary_key(self, pk=None):
        setattr(self, self.__primary_key_field, pk)

    def __check_deleted(self):
        if self.deleted:
            raise RuntimeError('object {c} {pk} is deleted'.format(
                c=self.__class__.__name__, pk=self._get_primary_key()))

    def storage_get(self, prop):
        """
        Get property value from the storage

        May be used in custom getters/setters for the external properties
        """
        return self._format_value(
            prop,
            storage.get_storage(self._property_map[prop]['store']).get_prop(
                self._get_primary_key(), prop))

    def storage_set(self, prop, value):
        """
        Save property value to the storage

        May be used in custom getters/setters for the external properties
        """
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
                if (tp == str or tp == bytes) and value is not None:
                    mn = p.get('min')
                    mx = p.get('max')
                    l = len(value)
                    if (mn is not None and l < mn) or (mx is not None and
                                                       l > mx):
                        raise ValueError
                elif (tp == int or tp == float) and value is not None:
                    mn = p.get('min')
                    mx = p.get('max')
                    if (mn is not None and value < mn) or (mx is not None and
                                                           value > mx):
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
                else:
                    return False

    def prepare_value(self, prop, value):
        """
        Prepare value before setting it to object property
        """
        return value

    def serialize(self, mode=None, allow_deleted=False):
        """
        Serialize object

        Args:
            mode: serialization mode. if not specified, all object properties
                are serialized
            allow_deleted: allow serialization of the deleted object
        """
        with self.__lock:
            if not allow_deleted: self.__check_deleted()
            return {
                key: self.serialize_prop(key)
                for key in self.__serialize_map[mode]
            }

    def serialize_prop(self, prop, target=None):
        """
        Serialize object property

        If method "serialize_{prop}" is defined in class, returns its value
            instead

        Args:
            prop: object property to serialize
            target: smartobject.SERIALIZE_SAVE or SERIALIZE_SYNC
        """
        try:
            return getattr(self, f'serialize_{prop}')(target=target)
        except AttributeError:
            return getattr(self, prop)

    def load(self, opts={}, **kwargs):
        """
        Load object data from the storage

        Calls self.after_load() method after loading

        Args:
            opts: passed to storage.load() as kwargs
        """
        with self.__lock:
            self.__check_deleted()
            logger.debug('Loading {c} {pk}'.format(c=self.__class__.__name__,
                                                   pk=self._get_primary_key()))
            for storage_id in self.__storages:
                self.set_prop(value={
                    key: value
                    for key, value in storage.get_storage(storage_id).load(
                        pk=self._get_primary_key(), **opts).items()
                    if not self._property_map[key].get('external')
                },
                              sync=False,
                              _allow_readonly=True)
                self.__modified[storage_id].clear()
            self.after_load(opts=opts)
            self.sync()

    def after_load(self, opts={}, **kwargs):
        """
        Called after load method
        """
        pass

    def sync(self, force=False):
        """
        Sync object data with synchroizer

        Args:
            force: force sync even if object is not modified
        """
        with self.__lock:
            pk = self._get_primary_key(_allow_null=False)
            self.__check_deleted()
            sync_tasks = {}
            for sync_id, props in self.__sync_map.items(
            ) if force else self.__modified_for_sync.items():
                sync_data = {
                    key: self.serialize_prop(key,
                                             target=constants.SERIALIZE_SYNC)
                    for key in chain(props, self.__sync_always[sync_id])
                }
                if not force: props.clear()
                if sync_data:
                    sync.get_sync(sync_id).sync(pk, sync_data)
        return True

    def save(self, force=False):
        """
        Save object data to storage

        Args:
            force: force save even if object is not modified
        """
        with self.__lock:
            self.__check_deleted()
            pk = self._get_primary_key()
            logger.debug('Saving {c} {pk}'.format(c=self.__class__.__name__,
                                                  pk=pk))
            for storage_id in self.__storages:
                if self.__modified[storage_id] or force:
                    data = {
                        key:
                        self.serialize_prop(key,
                                            target=constants.SERIALIZE_SAVE)
                        for key, props in self._property_map.items()
                        if 'store' in props and props['store'] == storage_id and
                        not props.get('external')
                    }
                    s = storage.get_storage(storage_id)
                    if pk is not None or s.generates_pk:
                        npk = s.save(pk=pk,
                                     data=data,
                                     modified=data if force else {
                                         key: data[key]
                                         for key in self.__modified[storage_id]
                                         if key in data
                                     })
                        self.__modified[storage_id].clear()
                    if pk is None and npk is not None:
                        pk = npk
                        self.set_prop(self.__primary_key_field,
                                      pk,
                                      _allow_readonly=True)

    def snapshot_create(self):
        """
        Create snapshot of object properties

        Snapshot is also saved to internal object variable

        Returns:
            snapshot dict
        """
        with self.__lock:
            snapshot = {
                key: getattr(self, key)
                for key, prop in self._property_map.items()
                if not prop.get('read-only') and not prop.get('external')
            }
            self.__snapshot = snapshot
            return snapshot.copy()

    def snapshot_rollback(self, snapshot=None):
        """
        Restore objct properties from the snapshot

        Args:
            snapshot: snapshot dict, if not defined, internal object variable
                is used
        Raises:
            ValueError: no snapshot data found
        """
        with self.__lock:
            if self.__snapshot is None and shapsnot is None:
                raise ValueError('No snapshot defined')
            self.set_prop(value=snapshot if snapshot else self.__snapshot)

    def delete(self, _call_factory=True):
        """
        Delete object
        """
        pk = self._get_primary_key()
        logger.debug('Deleting {c} {pk}'.format(c=self.__class__.__name__,
                                                pk=pk))
        if self._object_factory and _call_factory and pk is not None:
            self._object_factory.delete(pk)
        else:
            if not self.__deleted:
                self.__check_deleted()
                logger.info('Deleting {c} {pk}'.format(
                    c=self.__class__.__name__, pk=pk))
                with self.__lock:
                    self.__deleted = True
                    if pk is not None:
                        for storage_id in self.__storages:
                            storage.get_storage(storage_id).delete(
                                pk, self.__storage_map[storage_id])
                        for sync_id in self.__syncs:
                            sync.get_sync(sync_id).delete(pk)

    @property
    def deleted(self):
        """
        Is object deleted or not
        """
        return self.__deleted

    @property
    def alive(self):
        """
        Is object alive
        """
        return not self.__deleted
