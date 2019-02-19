#!/usr/bin/env python
# from setuptools import setup  # Using distutils, seems to be more flexible with build
from distutils.core import setup
import service_clients

LONG_DESCRIPTION = open('README.md').read()

CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Natural Language :: English',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Software Development :: Libraries :: Python Modules'
]

KEYWORDS = 'Python Service Clients'


deps = ['boto3==1.9.86', 'elasticsearch==6.3.1', 'redis==2.10.6']

# Manage requirements
setup(
    name='serviceclients',
    include_package_data=True,
    version=service_clients.__version__,
    description=KEYWORDS,
    long_description=LONG_DESCRIPTION,
    author='radzhome',
    author_email='radek@radtek.com',
    download_url='https://github.com/radzhome/python-service-clients',
    url='https://github.com/radzhome/python-service-clients',
    packages=['serviceclients', 'serviceclients.aws', 'serviceclients.cache', 'serviceclients.search'],
    package_dir={'serviceclients': 'serviceclients'},
    platforms=['Platform Independent'],
    license='BSD',
    classifiers=CLASSIFIERS,
    keywords=KEYWORDS,
    install_requires=deps
)
