#!/usr/bin/env python

from distutils.core import setup
from setuptools import setup

setup(name='DiMint_Server',
		version='1.0',
		description='Distributed in-memeory key-value storage',
		author='Ryu Jaeseong',
		author_email='jsryu21@snu.ac.kr',
		url='https://github.com/DiMint/DiMint_Server',
		packages=['dimint'],
		install_requires = [
			"pyzmq >= 14.0",
			],
		)
