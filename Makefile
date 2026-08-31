.PHONY: install lint test build all clean

PYTHON = python3
PIP = pip3

install:
	$(PIP) install --upgrade pip
	$(PIP) install pytest flake8 python-dateutil

lint:
	flake8 *.py --exclude=six.py --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 *.py --exclude=six.py --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

test:
	pytest tests/

build: lint test
	@echo "Build and tests completed successfully."

all: install build
