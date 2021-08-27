def _set_environment_variables() -> None:
    # To keep above OVH import to ensure env vars are set correctly
    from .common.config import PSQL_DSN  # noqa: F401


_set_environment_variables()

import subprocess  # noqa: E402
import tempfile  # noqa: E402
from datetime import datetime  # noqa: E402

from .common.ovh import BucketName, OVHClient  # noqa: E402

_AM_BUCKET: BucketName = 'am'


class DatabaseBackupError(Exception):
    pass


def _capture_backup() -> None:
    print('Capturing backup')
    completed_process = subprocess.run(
        ['heroku', 'pg:backups:capture', '--app', 'envinorma-back-office'], capture_output=True, text=True
    )
    if completed_process.returncode != 0:
        raise DatabaseBackupError(f'Error when executing Heroku backup: {completed_process.stderr}')


def _download_backup_from_heroku(filename: str) -> None:
    print('Downloading backup')
    completed_process = subprocess.run(
        ['heroku', 'pg:backups:download', '--app', 'envinorma-back-office', '--output', filename],
        capture_output=True,
        text=True,
    )
    if completed_process.returncode != 0:
        raise DatabaseBackupError(f'Error when executing Heroku backup: {completed_process.stderr}')


def _upload_backup_to_ovh(local_filename: str, remote_filename: str) -> None:
    print('Uploading backup')
    OVHClient.upload_document(_AM_BUCKET, local_filename, remote_filename)


def _backup_remote_filename() -> str:
    return f'backup/{datetime.now().isoformat()}.dump'


def backup_bo_database() -> None:
    _capture_backup()
    with tempfile.NamedTemporaryFile('w', prefix='bo-backup') as file_:
        _download_backup_from_heroku(file_.name)
        _upload_backup_to_ovh(file_.name, _backup_remote_filename())


if __name__ == '__main__':
    backup_bo_database()
