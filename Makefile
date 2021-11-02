test-and-lint:
	venv/bin/pytest --mypy-ignore-missing-imports
	venv/bin/flake8 --count --verbose --show-source --statistics
	venv/bin/black . --check -S -l 120
	venv/bin/isort . --profile black -l 120

generate-am:
	python3 -m tasks.data_build.generate_data --handle-ams

generate-ap:
	python3 -m tasks.data_build.generate_data --handle-aps

generate-installations-data:
	python3 -m tasks.data_build.generate_data --handle-installations-data

download-backup:
	sh scripts/download_backup.sh

init-db-from-backup:
	python3 scripts/recreate_db_from_backup.py

init-db-from-heroku-db:
	make download-backup
	make init-db-from-backup