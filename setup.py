"""
Data Act Broker Validation Component Install
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from setuptools.command.install import install
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()



setup(
    name='Data Act Validator',
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

    #keeping required packages in requirements.txt

    entry_points={
        'console_scripts': [
            'validator = dataactvalidator.scripts.baseScript:baseScript',
        ],
    }, 
)