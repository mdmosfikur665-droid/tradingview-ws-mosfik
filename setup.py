from setuptools import setup, find_packages

setup(
    name="tv_lib",
    version="0.1.1",
    packages=find_packages(),
    install_requires=[
        "websocket-client",
        "requests",
    ],
)
