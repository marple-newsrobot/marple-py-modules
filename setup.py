from setuptools import setup
from new_version import get_current_version

required = [
    'boto3>=1.4,<2',
    'glob2>=0.4,<1',
    'jsonschema>=2.5',
    'numpy>=1.11',
    'nw-connections>=1.2.0',
    'pandas>=0.20',
    'requests>=2,<3',
    'requests-cache>=0.4,<1',
    'requests-jwt>=0.4,<1',
    'simplejson>=3,<4',
    'pymongo>=3.3,<4'
]

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
