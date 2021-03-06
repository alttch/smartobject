# SmartObject

[SmartObject](https://github.com/alttch/smartobject) is a library to easily
manipulate object attributes with the API commands, store and synchronize
object data.

SmartObject is designed to quickly build IoT applications, but can be used in
any other applications, which require combining local and distributed object
storages and changing object properties via external API calls, automatically
validating incoming data.

<img src="https://img.shields.io/pypi/v/smartobject.svg" /> <img src="https://img.shields.io/badge/license-MIT-green" /> <img src="https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8-blue.svg" />

Mapped object attributes (called properties) can be automatically validated,
saved, loaded, serialized and synchronized with the external services.

SmartObject looks like ORM, but it's different from ORM: object properties can
be stored in storages of different type and combined together into a single
data object.

SmartObject has built-in storage engines for files (JSON, YAML, MessagePack and
CBOR) and databases: RDBMS via SQLAlchemy (can store objects) and Redis (can
handle external properties only).

Property values are automatically processed, validated and synchronized with
external services if required.

Note: all SmartObject methods are thread-safe (at least they should be :)

## Example:

You have a team of people with heart-rate sensors. Data for each person is
stored in local JSON files, heartbeat is stored in Redis database. How to
implement this with SmartObject? Just a few lines of code:

```python
{example_code}
```

The file *person.yml* is a property map for the *Person* object. It can be
loaded from the external YAML file or specified directly, as Python dict.

The map for the above example looks like:

```yaml
{example_map}
```

Pretty simple, isn't it? You define a map, SmartObject does the job!

## Install

```shell
pip3 install smartobject
```

## Documentation

Full documentation is available at https://smartobject.readthedocs.io/
