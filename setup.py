#!/usr/bin/env python

from distutils.core import setup
from setuptools import setup, find_packages

setup(name='ceiclient',
      version='0.1',
      description='Client tools for the OOI Common Execution Infrastructure',
      license='Apache 2.0',
      author='Nimbus team',
      author_email='nimbus@mcs.anl.gov',
      packages=['ceiclient'],
      entry_points={
          'console_scripts': [
              'ceictl=ceiclient.cli:start'
          ]
      },
      dependency_links = ['http://github.com/nimbusproject/cloudinit.d/tarball/master#egg=cloudinitd-1.2'],
      install_requires=['cloudinitd==1.2', 'dashi>=0.2.1', 'Jinja2', 'PyYAML',],
      tests_require=['nose', 'mock'],
      test_suite='nose.collector')
