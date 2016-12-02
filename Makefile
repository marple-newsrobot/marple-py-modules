
.PHONY: clean-pyc clean-build

install:
	virtualenv env
	source env/bin/activate
	pip install -r requirements.txt

clean-pyc:
	find . -name '*.pyc' -exec rm {} +
	find . -name '*.pyo' -exec rm {} +
	find . -name '*~' -exec rm {} +

tests: clean-pyc
	PYTHONPATH=. py.test tests --verbose

test: clean-pyc
	PYTHONPATH=. py.test $(file) --verbose


# Deploy new version tag
# Usage: make new_version v=0.0.4 msg="Made some changes"
new_version:
	python new_version.py ${v} "${msg}"
	git add CURRENT_VERSION.txt CHANGES.txt
	git commit -m "New version, ${v}: ${msg}"
	git tag -a v${v} -m '${msg}'
	git push --tags

