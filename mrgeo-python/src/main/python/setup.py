from codecs import open
from os import path
from setuptools import setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pymrgeo',
    packages=['pymrgeo'],  # this must be the same as the name above
    version='1.0.2',  # This value must change to generate a new bundle
    description='MrGeo (pronounced "Mister Geo") is an open source geospatial toolkit designed to provide raster-based geospatial processing capabilities performed at scale. MrGeo enables global geospatial big data image processing and analytics.',
    long_description=long_description,
    author='Tim Tisler and Dave Johnson',
    author_email='tim.tisler@digitalglobe.com and dave.johnson@digitalglobe.com',
    url='https://github.com/ngageoint/mrgeo',  # use the URL to the github repo
    keywords='geospatial toolkit hadoop apache spark raster-based',
    license='Apache 2.0',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
