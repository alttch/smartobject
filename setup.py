__version__ = '0.1.15'

import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(name='smartobject',
                 version=__version__,
                 author='Altertech',
                 author_email='div@altertech.com',
                 description='Smart Objects',
                 long_description=long_description,
                 long_description_content_type='text/markdown',
                 url='https://github.com/alttch/smartobject',
                 packages=setuptools.find_packages(),
                 license='MIT',
                 install_requires=['jsonschema', 'pyyaml', 'pyaltt2'],
                 classifiers=('Programming Language :: Python :: 3',
                              'License :: OSI Approved :: MIT License',
                              'Topic :: Software Development :: Libraries'))
