from distutils.core import setup

setup(
  name='datomic',
  version='0.1.2dev',
  description='Interface to the Datomic REST API',
  #long_description=open('README.md').read(),
  author='Tony Landis',
  author_email='tony.landis@gmail.com',
  license="Apache 2",
  url='https://github.com/tony-landis/datomic-py',
  install_requires=[
    'edn_format',
    'urllib3',
    'termcolor',
  ],
  packages = ['datomic', ],
)
