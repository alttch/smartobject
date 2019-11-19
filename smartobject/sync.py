syncs = {}


def define_sync(sync, id=None):
    if id is not None and not isinstance(id, str) and not isinstance(id, int):
        raise ValueError('Sync ID must be string or integer')
    syncs[id] = sync


def get_sync(id=None):
    try:
        return syncs[id]
    except KeyError:
        raise RuntimeError(f'Sync "{id}" is not defined')


class AbstractSync:

    def sync(self, pk, data={}, **kwargs):
        raise RuntimeError('not implemented')

    def delete(self, pk, **kwargs):
        raise RuntimeError('not implemented')


class DummySync:

    def sync(self, pk, data={}, **kwargs):
        return True

    def delete(self, pk, **kwargs):
        return True
