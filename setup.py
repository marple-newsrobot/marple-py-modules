from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(name='marple',
      version='0.1',
      description='Shared Marple python modules',
      url='http://github.com/marple-newsrobot/marple-py-modules',
      author='Journalism Robotics Stockholm',
      author_email='stockholm@jplusplus.org',
      license='MIT',
      packages=['marple'],
      install_requires=required,
      zip_safe=False)