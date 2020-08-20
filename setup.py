#! /usr/bin/env python3
import os
from setuptools import setup, find_namespace_packages

BASE_DIR = os.path.dirname(__file__)
DEPENDENCIES = list(filter(
    lambda s: not s.startswith('git+ssh'), open(os.path.join(BASE_DIR, 'requirements.txt')).readlines()
))

setup(
    name='dataprocessing-master-thesis',
    version='0.0.1',
    author='AltR',
    author_email='altieris.marcelino@gmail.com',
    license=open(os.path.join(BASE_DIR, 'LICENSE')).read(),
    packages=find_namespace_packages(include=['dataprocessing.*']),
    namespace_packages=['dataprocessing'],
    install_requires=DEPENDENCIES,
    zip_safe=False,
    python_requires='>=3.6'
)
