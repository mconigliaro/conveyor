try:
  from setuptools import setup
except ImportError:
  from distutils.core import setup

setup(
  name = 'conveyor',
  version = '0.1.0',
  description = 'Conveyor is an application deployment framework',
  author = 'Michael T. Conigliaro',
  author_email = 'mike [at] conigliaro [dot] org',
  url = 'http://conigliaro.org',
  download_url = 'http://conigliaro.org',
  install_requires = ['nose', 'zookeeper'],
  packages = ['conveyor'],
  scripts = ['bin/conveyor']
)
