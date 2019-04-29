STORAGE_ACCOUNT=<fill this in>
STORAGE_KEY=<fill this in too>

# local overrides
-include .env

# Run tests
tables:
	python -u test_tables.py

queues:
	python -u test_queues.py

blobs:
	python -u test_blobs.py