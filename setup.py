# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '18 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from setuptools import setup, find_packages

# Populate long description setting with content of README
#
# Use markdown format read me file as GitHub will render it automatically
# on package page
with open("README.md") as readme_file:
    _long_description = readme_file.read()

setup(
    name='fbs',
    version=0.1,
    description='File Based Search scanning code',
    author='Richard Smith',
    author_email='richard.d.smith@stfc.ac.uk',
    url='https://github.com/cedadev/ceda-fbs/',
    long_description=_long_description,
    long_description_content_type='text/markdown',
    license='BSD - See ceda_example/LICENSE file for details',
    packages=find_packages('python/src'),
    package_dir={'': 'python/src'},
    package_data={
        'fbs': [
            'LICENSE',
        ],
    },
    install_requires=[
        'elasticsearch<7.0.0',
    ],

    # This qualifier can be used to selectively exclude Python versions -
    # in this case early Python 2 and 3 releases
    python_requires='>=2.7.0',

    # See:
    # https://www.python.org/dev/peps/pep-0301/#distutils-trove-classification
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Systems Administration :: Authentication/Directory',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)