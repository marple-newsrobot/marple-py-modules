from setuptools import setup
from version import get_current_version

# Get list of requirements
with open('requirements.txt') as f:
    required = f.read().splitlines()

# Get current version
current_version = get_current_version()

setup(name='marple',
      version=current_version,
      description='Shared Marple python modules',
      url='http://github.com/marple-newsrobot/marple-py-modules',
      author='Journalism Robotics Stockholm',
      author_email='stockholm@jplusplus.org',
      license='MIT',
      packages=['marple'],
      include_package_data=True,
      install_requires=required,
      zip_safe=False)