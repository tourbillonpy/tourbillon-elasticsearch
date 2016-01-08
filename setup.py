import sys

from setuptools import find_packages, setup

PY34_PLUS = sys.version_info[0] == 3 and sys.version_info[1] >= 4

exclude = ['tourbillon.elasticsearch.elasticsearch2'
           if PY34_PLUS else 'tourbillon.elasticsearch.elasticsearch']

install_requires = []

if not PY34_PLUS:
    install_requires.append('requests>=2.7.0')
else:
    install_requires.append('aiohttp>=0.17.2')


setup(
    name='tourbillon-elasticsearch',
    version='0.4.2',
    description='A tourbillon plugin for collecting metrics from'
    ' Elasticsearch.',
    packages=find_packages(exclude=exclude),
    install_requires=install_requires,
    zip_safe=False,
    namespace_packages=['tourbillon'],
    author='The Tourbillon Team',
    author_email='tourbillonpy@gmail.com',
    url='https://github.com/tourbillonpy/tourbillon-elasticsearch',
    license='ASF',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: System :: Monitoring',
    ],
    keywords='monitoring metrics agent influxdb elasticsearch',
)
