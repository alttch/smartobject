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
        self._object_class = object_class
        self.__lock = threading.RLock()
        self.autosave = autosave

    def create(self, obj=None, load=False, save=None, override=False, **kwargs):
        """
        Create new Smart Object in factory

        Args:
            obj: append existing Smart Object
            load: call obj.load() before append
            save: call obj.save() before append
            override: allow overriding existing objects
            **kwargs: sent to object constructor as-is

        Raises:
            RuntimeError: if object with such primary key already exists and
            override is False
        """
        if obj is None:
            obj = self._object_class(**kwargs)
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
            logger.debug(
                f'Added object {self._object_class.__name__} {pk} to factory')

        return obj

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

    def get(self, pk=None):
        """
        Get Smart Object from factory

        Args:
            pk: object primary key. If not specified, dict of all objects is
                returned
        Raises:
            KeyError: if object with such pk doesn't exist
        """
        with self.__lock:
            return self._objects[pk] if pk else self._objects.copy()

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

    def load_all(self,
                 storage_id=None,
                 load_opts={},
                 override=False,
                 **kwargs):
        """
        Load all objects from specified storage

        Args:
            storage_id: storage ID
            load_opts: dict of kwargs, passed to storage.load_all() method
            override: allow overriding existing objects
            **kwargs: passed to object constructor as-is
        """
        from . import storage
        with self.__lock:
            for d in storage.get_storage(storage_id).load_all(**load_opts):
                if 'data' in d:
                    logger.debug(
                        f'Creating object {self._object_class.__name__}')
                    o = self._object_class(**kwargs)
                    o.set_prop(d['data'],
                               _allow_readonly=True,
                               sync=False,
                               save=False)
                    o.after_load(**d.get('info', {}))
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

    def delete(self, pk):
        """
        Delete object and remove it from the factory

        Args:
            pk: object primary key, required
        """
        o = self.get(pk)
        self.remove(pk, _obj=o)
        o.delete(_call_factory=False)

    def clear(self):
        """
        Remove all objects in factory
        """
        logger.debug(f'Clearing factory objects {self._object_class.__name__}')
        with self.__lock:
            self._objects.clear()

    def cleanup(self, storage_id=None, **kwargs):
        """
        Cleanup object storage

        Deletes from the specified storage all stored objects, which are not in
        factory

        Args:
            storage_id: storage id to cleanup or None for default storage
            **kwargs: passed to storage.cleanup() as-is
        """
        from . import storage
        logger.debug(
            f'{self._object_class.__name__} storage {storage_id} cleanup')
        with self.__lock:
            return storage.get_storage(storage_id).cleanup(
                list(self.get()), **kwargs)

    def remove(self, pk, _obj=None):
        """
        Remove object from the factory

        Args:
            pk: object primary key, required
        """
        logger.debug(
            f'Removing objects {self._object_class.__name__} {pk} from factory')
        with self.__lock:
            if _obj is None:
                _obj = self.get(pk)
            _obj._object_factory = None
            del self._objects[pk]
