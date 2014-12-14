from setuptools import setup, find_packages

setup(
    name='dimint_overlord',
    version='0.1.0',
    packages=find_packages(),
    package_data={
        'dimint_overlord': ['*.config', 'dimint_overlord/*.config'],
        },
    install_requires=[
        'pyzmq',
        'kazoo',
        'psutil',
    ],
    entry_points='''
        [console_scripts]
        dimint_overlord=dimint_overlord.Overlord:main
        dimint=dimint_overlord.dimint:main
    ''',
)
