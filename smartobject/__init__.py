__version__ = '0.1.15'

from . import config

from .smartobject import SmartObject
from .factory import SmartObjectFactory

from .storage import get_storage, define_storage, purge, DummyStorage
from .storage import AbstractStorage, AbstractFileStorage
from .storage import JSONStorage, YAMLStorage
from .storage import PickleStorage, MessagePackStorage, CBORStorage
from .storage import SQLAStorage, RedisStorage

from .sync import AbstractSync, DummySync, define_sync, get_sync

from .constants import SERIALIZE_SAVE, SERIALIZE_SYNC
