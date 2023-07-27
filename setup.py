#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(
    name='lambrecht',
    version='0.1',
    description='Web interface for Lambrecht meteo weather station',
    author='Tim-Oliver Husser',
    author_email='thusser@uni-goettingen.de',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'lambrecht-web=lambrecht.web:main'
        ]
    },
    package_data={'lambrecht': ['*.html', 'static_html/*.css']},
    include_package_data=True,
    install_requires=['tornado', 'apscheduler', 'pyserial', 'numpy']
)
