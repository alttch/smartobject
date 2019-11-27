import threading
import logging

logger = logging.getLogger('smartobject')


class SmartObjectFactory:
    """
    Object factory class for Smart Objects
    """

    def __init__(self, object_class=None, autosave=False):
        """
        Initialize Smart Object factory

        Args:
            object_class: Object class the factory is for
            autosave: Auto save objects after creation
        """
        self._objects = {}
        self._objects_by_prop = {}
        self._object_class = object_class
        self.__lock = threading.RLock()
        self.autosave = autosave

    def add_index(self, prop):
        """
        Add index property

        The factory can index objects by additional index props.

        Condition using index props:

            - prop should be either read-only or not changed after
              first set
            - factory index props should be defined before objectas are added
              to factory

        If object property is set after it was added to factory, it can be
        reindexed with factory.reindex() method

        Args:
            prop: object prop name or list of prop names

        Raises:
            RuntimeError: if factory already contains objects
            ValueError: if index already defined
        """
        with self.__lock:
            for p in prop if isinstance(prop, list) or isinstance(
                    prop, tuple) else [prop]:
                if self._objects:
                    raise RuntimeError('Factory already contains objects')
                if p not in self._objects_by_prop:
                    self._objects_by_prop[p] = {}
                else:
                    raise ValueError('Index already defined')

    def create(self, opts={}, obj=None, load=False, save=None, override=False):
        """
        Create new Smart Object in factory

        Args:
            obj: append existing Smart Object
            load: call obj.load() before append
            save: call obj.save() before append
            override: allow overriding existing objects
            opts: sent to object constructor as kwargs

        Raises:
            RuntimeError: if object with such primary key already exists and
            override is False
        """
        if obj is None:
            obj = self._object_class(**opts)
        if load:
            obj.load()
        if (self.autosave and save is not False) or save:
            obj.save()
        pk = obj._get_primary_key()
        if pk is None:
            raise ValueError('Object has no primary key')
        with self.__lock:
            if pk in self._objects and not override:
                raise RuntimeError(f'Object already exists: {pk}')
            self._objects[pk] = obj
            obj._object_factory = self
            self.reindex(pk)
            logger.debug(
                f'Added object {self._object_class.__name__} {pk} to factory')
        return obj

    def reindex(self, obj):
        """
        Reindex object, stored in factory

        Args:
            obj: object or object primary key, required
        """
        with self.__lock:
            if not isinstance(obj, self._object_class):
                obj = self.get(obj)
            for p in self._objects_by_prop:
                val = getattr(obj, p)
                if val is not None:
                    self._objects_by_prop[p].setdefault(val, set()).add(obj)

    def append(self, obj, load=False, save=None, override=False):
        """
        Append object to factory

        Same as create(obj=obj)

        Args:
            obj: append existing Smart Object
            load: call obj.load() before append
            save: call obj.save() before append
            override: allow overriding existing objects
        """
        return self.create(obj=obj, load=load, save=save, override=override)

    def get(self, key=None, prop=None):
        """
        Get Smart Object from factory

        Args:
            key: object key. If not specified, dict of all objects is
                returned
            prop: object prop (should be indexed). If no prop specified, object
                is looked up by primary key
        Raises:
            KeyError: if object with such key doesn't exist (for primary key)
        Returns:
            For primary key: single object is returned. For another prop: list
            of objects. The list can be empty
        """
        with self.__lock:
            if not key:
                return self._objects.copy()
            elif prop is not None:
                return list(self._objects_by_prop[prop][key])
            else:
                return self._objects[key]

    def load(self, pk=None):
        """
        Call load method of the specified object

        Args:
            pk: object primary key. If not specified, load() method is called
                for all objects in factory
        """
        if pk:
            with self.__lock:
                self.get(pk).load()
        else:
            for i, o in self.get().items():
                o.load()

    def load_all(self, storage_id=None, load_opts={}, override=False, opts={}):
        """
        Load all objects from specified storage

        Args:
            storage_id: storage ID
            load_opts: dict of kwargs, passed to storage.load_all() method
            override: allow overriding existing objects
            **kwargs: passed to object constructor as kwargs
        """
        from . import storage
        with self.__lock:
            for d in storage.get_storage(storage_id).load_all(**load_opts):
                if 'data' in d:
                    logger.debug(
                        f'Creating object {self._object_class.__name__}')
                    o = self._object_class(**opts)
                    o.set_prop(d['data'],
                               _allow_readonly=True,
                               sync=False,
                               save=False)
                    o.after_load(opts=d.get('info', {}))
                    o.sync()
                    self.create(obj=o, override=override, save=False)

    def save(self, pk=None, force=False):
        """
        Call save method of the specified object

        Args:
            pk: object primary key. If not specified, save() method is called
                for all objects in factory
        """
        if pk:
            with self.__lock:
                self.get(pk).save(force)
        else:
            for i, o in self.get().items():
                o.save(force=force)

    def sync(self, pk=None, force=False):
        """
        Call sync method of the specified object

        Args:
            pk: object primary key. If not specified, save() method is called
                for all objects in factory
        """
        if pk:
            with self.__lock:
                self.get(pk).sync(force)
        else:
            for i, o in self.get().items():
                o.sync(force=force)

    def set_prop(self, pk, *args, **kwargs):
        """
        Call set_prop method of the specified object

        Args:
            pk: object primary key, required. Other arguments are passed to
                SmartObject.set_prop as-is
        """
        return self.get(pk).set_prop(*args, **kwargs)

    def serialize(self, pk, *args, **kwargs):
        """
        Serialize object
        """
        return self.get(pk).serialize(*args, **kwargs)

    def delete(self, obj):
        """
        Delete object and remove it from the factory

        Args:
            obj: object or object primary key, required
        """
        with self.__lock:
            if not isinstance(obj, self._object_class):
                obj = self.get(obj)
            self.remove(obj=obj)
            obj.delete(_call_factory=False)

    def clear(self):
        """
        Remove all objects in factory
        """
        logger.debug(f'Clearing factory objects {self._object_class.__name__}')
        with self.__lock:
            self._objects.clear()

    def cleanup_storage(self, storage_id=None, opts={}):
        """
        Cleanup object storage

        Deletes from the specified storage all stored objects, which are not in
        factory

        Args:
            storage_id: storage id to cleanup or None for default storage
            opts: passed to storage.cleanup() as kwargs
        """
        from . import storage
        logger.debug(
            f'{self._object_class.__name__} storage {storage_id} cleanup')
        with self.__lock:
            return storage.get_storage(storage_id).cleanup(
                list(self.get()), **opts)

    def remove(self, obj):
        """
        Remove object from the factory

        Args:
            obj: object or object primary key, required
        """
        with self.__lock:
            if not isinstance(obj, self._object_class):
                obj = self.get(obj)
            pk = obj._get_primary_key()
            logger.debug('Removing object ' +
                         f'{self._object_class.__name__} {pk} from factory')
            obj._object_factory = None
            for p in self._objects_by_prop:
                val = getattr(obj, p)
                if val is not None:
                    try:
                        self._objects_by_prop[p][val].remove(obj)
                    except (ValueError, KeyError):
                        pass
            del self._objects[pk]
