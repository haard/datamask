#!/usr/bin/env python3

from distutils.core import setup

setup(name='pgdatacleaner',
      version='1.0',
      description='Data cleaning/masking for PostgreSQL databases',
      author='Fredrik Håård',
      author_email='fredrik@metallapan.se',
      url='https://github.com/haard/pgdatacleaner',
      packages=['pgdatacleaner'],
      entry_points={
          'console_scripts': [
              'dataclean=pgdatacleaner.cleaner:main',
              'datadict=pgdatacleaner.datadict:main',
      
          ]
      },
      install_requires=['psycopg2-binary'],
      )
