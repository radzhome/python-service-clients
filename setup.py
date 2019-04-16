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


requires = [
    'boto3>=1.9.0,<3.0.0',
    'elasticsearch>=6.3.1,<7.0.0',
    'redis>=2.10.6,<3.0.0'

]

extra_require = {
    'rabbit':  ['pika>=1.0.1', ],
    'boto':  ['boto>=2.49.0', ],  # TODO: drop
}


# Manage requirements
setup(
    name='service_clients',
    include_package_data=True,
    version=service_clients.__version__,
    description=KEYWORDS,
    long_description=LONG_DESCRIPTION,
    author='radzhome',
    author_email='radek@radtek.ca',
    download_url='https://github.com/radzhome/python-service-clients',
    url='https://github.com/radzhome/python-service-clients',
    packages=['service_clients', 'service_clients.aws', 'service_clients.cache', 'service_clients.search'],
    package_dir={'service_clients': 'service_clients'},
    platforms=['Platform Independent'],
    license='BSD',
    classifiers=CLASSIFIERS,
    keywords=KEYWORDS,
    install_requires=requires
)
