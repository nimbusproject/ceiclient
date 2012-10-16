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
      install_requires=['cloudinitd==1.2', 'dashi==0.2', 'Jinja2', 'PyYAML', 'mock', 'nose'],
      test_suite='nose.collector')
