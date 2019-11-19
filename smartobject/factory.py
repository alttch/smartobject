import threading


class SmartObjectFactory:

    def __init__(self, object_class):
        self._objects = {}
        self._object_class = object_class
        self.__lock = threading.RLock()

    def create(self, obj=None, load=True, override=False, **kwargs):
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

    def get(self, pk=None):
        with self.__lock:
            return self._objects[pk] if pk else self._objects.copy()

    def load(self, pk=None):
        if pk:
            with self.__lock:
                self.get(pk).load()
        else:
            for i, o in self.get().items():
                o.load()

    def save(self, pk=None, force=False):
        if pk:
            with self.__lock:
                self.get(pk).save(force)
        else:
            for i, o in self.get().items():
                o.save(force=force)

    def sync(self, pk=None, force=False):
        if pk:
            with self.__lock:
                self.get(pk).sync(force)
        else:
            for i, o in self.get().items():
                o.sync(force=force)

    def set_prop(self, pk, *args, **kwargs):
        self.get(pk).set_prop(*args, **kwargs)

    def delete(self, pk):
        o = self.get(pk)
        with self.__lock:
            del self._objects[pk]
        o.delete(_call_factory=False)
