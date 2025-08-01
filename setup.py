"""
Data Broker Backend Install
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import os
from os import path
import inspect
from pip.req import parse_requirements
from pip.download import PipSession

here = path.abspath(path.dirname(__file__))

# Get path from current file location
dir_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
requirements_path = os.path.join(dir_path, "requirements.txt")
# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements(requirements_path, session=PipSession())

# Create the list of requirements
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name="dataactbrokerbackend",
    version="0.0.1",
    description="Data Broker Backend",
    long_description="The Data Broker API powers the Data Broker's data submission process.",
    url="https://github.com/fedspendingtransparency/data-act-broker-backend.git",
    author="US Treasury",
    author_email="databroker@fiscal.treasury.gov",
    license="CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
        "Programming Language :: Python :: 3.x",
    ],
    keywords="Data Broker Backend Setup",
    packages=find_packages(),
    install_requires=reqs,
    entry_points={
        "console_scripts": [
            "webbroker = dataactbroker.scripts.initialize:options",
            "validator = dataactvalidator.scripts.initialize:options",
        ],
    },
)
