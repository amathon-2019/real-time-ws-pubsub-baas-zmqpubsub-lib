# coding=utf-8

from setuptools import setup

setup(
    name='zmq_pubsub',
    version='0.0.1',
    packages=['zmq_pubsub'],
    install_requires=[
        'aiohttp==3.5.4',
        'aioredis==1.2.0',
        'aiozmq==0.7.1',
    ]
)
