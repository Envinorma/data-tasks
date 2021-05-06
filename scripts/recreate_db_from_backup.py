"""
Empty database and recreates it from most recent available backup.
"""
import os
import pathlib
import subprocess
from datetime import datetime

import psycopg2

from config import PSQL_DSN

_CONNECTION = psycopg2.connect(PSQL_DSN)


def _drop_tables():
    commands = (
        """DROP TABLE IF EXISTS am_status""",
        """DROP TABLE IF EXISTS initial_am""",
        """DROP TABLE IF EXISTS structured_am""",
        """DROP TABLE IF EXISTS parametrization""",
    )
    cur = _CONNECTION.cursor()
    for command in commands:
        cur.execute(command)
    _CONNECTION.commit()
    cur.close()


def _get_most_backup_filename(folder: str) -> str:
    files = os.listdir(folder)
    if not files:
        raise ValueError('No backup files found')
    return sorted(files, key=lambda x: datetime.strptime(x, '%Y-%m-%d-%H-%M.dump'))[-1]


def _restore_backup(backup_file: str):
    subprocess.Popen(['pg_restore', '-d', 'am_bu', '--role', 'remidelbouys', backup_file])


_BACKUP_FOLDER = pathlib.Path('.').parent.joinpath('backups')


def run():
    if '0.0.0.0:5432' not in PSQL_DSN:
        raise ValueError('Cannot drop remote tables.')
    _drop_tables()
    filename = _get_most_backup_filename(str(_BACKUP_FOLDER))
    _restore_backup(str(_BACKUP_FOLDER.joinpath(filename)))


if __name__ == '__main__':
    run()
