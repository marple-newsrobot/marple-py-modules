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
      install_requires=['Babel==2.3.4', 'Jinja2==2.8', 'MarkupSafe==0.23', 'PyJWT==1.4.2', 'Pygments==2.1.3', 'SQLAlchemy==1.1.4', 'Sphinx==1.4.8', 'alabaster==0.7.9', 'csvkit==0.9.1', 'dbf==0.94.003', 'docutils==0.12', 'functools32==3.2.3-2', 'imagesize==0.7.1', 'jdcal==1.3', 'jsonschema==2.5.1', 'marple-py==0.1', 'numpy==1.11.2', 'openpyxl==2.2.0-b1', 'pandas==0.19.1', 'py==1.4.31', 'pymongo==3.3.1', 'pytest==3.0.3', 'python-dateutil==2.2', 'pytz==2016.7', 'requests==2.12.1', 'requests-jwt==0.4', 'simplejson==3.10.0', 'six==1.10.0', 'snowballstemmer==1.2.1', 'wsgiref==0.1.2', 'xlrd==1.0.0'],
      zip_safe=False)