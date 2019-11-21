import threading


class SmartObjectFactory:
    """
    Object factory class for Smart Objects
    """

    def __init__(self, object_class=None):
        """
        Initialize Smart Object factory

        Args:
            object_class: Object class the factory is for
        """
        self._objects = {}
        self._object_class = object_class
        self.__lock = threading.RLock()

    def create(self, obj=None, load=False, override=False, **kwargs):
        """
        Create new Smart Object in factory

        Args:
            obj: append existing Smart Object
            load: call obj.load() before append
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
        pk = obj._get_primary_key()
        with self.__lock:
            if pk in self._objects and not override:
                raise RuntimeError(f'Object already exists: {pk}')
            self._objects[pk] = obj
            obj._object_factory = self
        return obj

    def append(self, obj, load=False, override=False):
        """
        Append object to factory

        Same as create(obj=obj)

        Args:
            obj: append existing Smart Object
            load: call obj.load() before append
            override: allow overriding existing objects
        """
        return self.create(obj=obj, load=load, override=override)

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

    def load_from_files(self,
                        storage_id=None,
                        pattern=None,
                        override=False,
                        **kwargs):
        """
        Load all objects from specified file storage

        Args:
            storage_id: storage ID
            pattern: file pattern
            override: allow overriding existing objects
            **kwargs: passed to object constructor as-is
        """
        from . import storage
        with self.__lock:
            for f in storage.get_storage(storage_id).list(**kwargs):
                o = self._object_class(**kwargs)
                o.load(fname=str(f), allow_empty=False)
                self.create(obj=o, override=override)

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
        with self.__lock:
            self._objects.clear()

    def remove(self, pk, _obj=None):
        """
        Remove object from the factory

        Args:
            pk: object primary key, required
        """
        with self.__lock:
            if _obj is None:
                _obj = self.get(pk)
            _obj._object_factory = None
            del self._objects[pk]
