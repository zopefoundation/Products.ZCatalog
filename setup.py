##############################################################################
#
# Copyright (c) 2010 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

from setuptools import setup, find_packages

setup(
    name='Products.ZCatalog',
    version='4.2.dev0',
    url='https://github.com/zopefoundation/Products.ZCatalog',
    license='ZPL 2.1',
    description="Zope's indexing and search solution.",
    author='Zope Foundation and Contributors',
    author_email='zope-dev@zope.org',
    long_description=(open('README.rst').read() + '\n' +
                      open('CHANGES.rst').read()),
    packages=find_packages('src'),
    namespace_packages=['Products'],
    package_dir={'': 'src'},
    classifiers=[
        "Development Status :: 6 - Mature",
        "Environment :: Web Environment",
        "Framework :: Zope :: 4",
        "License :: OSI Approved :: Zope Public License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
    ],
    keywords='Zope catalog index search data',
    install_requires=[
        'setuptools',
        'six',
        'AccessControl >= 4.0a4',
        'Acquisition',
        'BTrees',
        'DateTime',
        'DocumentTemplate',
        'ExtensionClass',
        'Missing',
        'Persistence',
        'Record',
        'RestrictedPython',
        'zExceptions',
        'ZODB',
        'Zope2 >= 4.0a5',
        'zope.deferredimport',
        'zope.dottedname',
        'zope.globalrequest',
        'zope.interface',
        'zope.schema',
        'zope.testing',
    ],
    include_package_data=True,
    zip_safe=False,
)
