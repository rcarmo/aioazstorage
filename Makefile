STORAGE_ACCOUNT=<fill this in>
STORAGE_KEY=<fill this in too>

# local overrides
-include .env

# Run tests
tables:
	python -u test_tables.py

queues:
	python -u test_queues.py

containers:
	python -u test_blobs.py containers

blobs:
	python -u test_blobs.py blobs

deps:
	pip install -U -r requirements.txt