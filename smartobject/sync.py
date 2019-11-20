syncs = {}


def define_sync(sync, id=None):
    """
    Define new synchronizer

    The synchonizer must implement methods of AbstractSync class

    Args:
        sync: synchronizer object
        id: sync id, if not specified, default sync is defined
    """
    if id is not None and not isinstance(id, str) and not isinstance(id, int):
        raise ValueError('Sync ID must be string or integer')
    syncs[id] = sync


def get_sync(id=None):
    """
    Get synchronizer

    Args:
        id: sync id, if not specified, default sync is returned
    """
    try:
        return syncs[id]
    except KeyError:
        raise RuntimeError(f'Sync "{id}" is not defined')


class AbstractSync:
    """
    Abstract synchronizer class which can be used as synchronizer template
    """

    def sync(self, pk, data={}, **kwargs):
        """
        Sync object data

        The method is called every time object requests to be synchronized

        Args:
            pk: object primary key
            data: object data to sync
        """
        raise RuntimeError('not implemented')

    def delete(self, pk, **kwargs):
        """
        Delete object data

        The method is called when the object is deleted

        Args:
            pk: object primary key
        """
        raise RuntimeError('not implemented')


class DummySync:
    """
    Dummy synchronizer class with empty methods

    Does nothing, useful for testing
    """

    def sync(self, pk, data={}, **kwargs):
        return True

    def delete(self, pk, **kwargs):
        return True
