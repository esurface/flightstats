#!/usr/bin/env python

import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

def path(p):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), p)

long_description = ''

try:
    long_description += open(path('README.rst')).read()
    long_description += '\n\n' + open(path('CHANGES.rst')).read()
except IOError:
    pass

version = '0.0.1'

requirements = [
    'apiclient'
]

setup(name='flightstatsclient',
      version=version,
      description="API client for Cirium Flightstats.",
      long_description=long_description,
      classifiers=[
          'Environment :: Web Environment',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Topic :: Internet :: WWW/HTTP',
          'Topic :: Software Development :: Libraries',
      ],
      keywords='api client flightstats urllib3 keepalive threadsafe http rest',
      author='Erik surface',
      author_email='erik.surface@gmail.com',
      url='https://github.com/esurface/flightstatsclient',
      license='MIT',
      packages=['flightstatsclient'],
      install_requires=requirements,
      )
