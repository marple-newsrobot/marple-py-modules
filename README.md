# Marple Python Modules

A python package for modules shared across the Marple project.


### Get started

`make install` (sets up local environment and installs dependencies)

### Install package

`pip install git+ssh://git@github.com/marple-newsrobot/marple-py-modules.git`

...or, using https:

`pip install git+https://github.com/marple-newsrobot/marple-py-modules.git`

### Run tests

`make tests` (run all tests)

`make test file=path/to/test` (run specific test)

### Deploy

`make deploy_new_version v=0.0.4 msg="Made some changes"`

The current version is defined in `CURRENT_VERSION.txt`. This file is updated with this make command.

### Update docs

`cd docs`

`make html`

### Notes to self

Useful links:

- Private packages, versioning etc. http://jtushman.github.io/blog/2013/06/17/sharing-code-across-applications-with-python/#3
- Generate documentation with Sphinx: http://gisellezeno.com/tutorials/sphinx-for-python-documentation.html

