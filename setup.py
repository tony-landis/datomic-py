from distutils.core import setup

setup(
  name='datomic-py',
  version='0.1dev',
  description='Interface to the Datomic REST API',
  long_description=open('README.md').read(),
  author='Tony Landis',
  author_email='tony.landis@gmail.com',
  url='https://github.com/tony-landis/datomic-py',
  install_requires=[
    'edn_format'
    'urllib3'
    'termcolor'
  ],
  packages = ['datomic'],
)
