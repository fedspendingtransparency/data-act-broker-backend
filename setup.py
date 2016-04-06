"""
Data Act Broker Validation Component Install
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from setuptools.command.install import install
# To use a consistent encoding
import os
from os import path
import inspect
from codecs import open
from pip.download import PipSession
from pip.req import parse_requirements



here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Get path from current file location
dirPath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
requirementsPath = os.path.join(dirPath,"requirements.txt")
# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements(requirementsPath,session=PipSession())

# Create the list of requirements
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='dataactvalidator',
    version='0.0.1',
    description='Validation service for the Data Act Broker',
    long_description=long_description,
    url='https://github.com/fedspendingtransparency/data-act-validator.git',
    author='US Treasury',
    author_email='na@na.com',
    license='CC0 1.0 Universal (CC0 1.0) Public Domain Dedication',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication',
        'Programming Language :: Python :: 2.7'

    ],
    keywords='dataAct validator setup',
    packages=find_packages(),
    install_requires=reqs,
    dependency_links=[
      'git+https://git@github.com/fedspendingtransparency/data-act-core.git@configuration#egg=dataactcore-0.0.1',
    ],
    entry_points={
        'console_scripts': [
            'validator = dataactvalidator.scripts.initialize:options',
        ],
    }, 
)