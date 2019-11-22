SERIALIZE_SAVE = 0
SERIALIZE_SYNC = 1

PROPERTY_MAP_SCHEMA = {
    'type': 'object',
    'patternProperties': {
        '^': {
            'type': 'object',
            'properties': {
                'pk': {
                    'type': 'boolean'
                },
                'read-only': {
                    'type': 'boolean'
                },
                'external': {
                    'type': 'boolean'
                },
                'type': {},
                'default': {},
                'choices': {
                    'type': 'array'
                },
                'min': {
                    'type': 'number',
                },
                'max': {
                    'type': 'number',
                },
                'accept-hex': {
                    'type': 'boolean'
                },
                'serialize': {
                    'type': ['array', 'string']
                },
                'store': {
                    'type': ['boolean', 'string']
                },
                'sync': {
                    'type': ['boolean', 'string']
                },
                'sync-always': {
                    'type': ['boolean']
                },
                'log-level': {
                    'type': 'number',
                },
                'log-hide-value': {
                    'type': 'boolean'
                }
            },
            'additionalProperties': False
        }
    }
}
