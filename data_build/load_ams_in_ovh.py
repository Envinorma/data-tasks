import shutil
import tempfile
from datetime import datetime

from ..common.ovh_upload import init_swift_service, upload_document
from .build.build_am_repository import generate_am_repository
from .build.build_ams import generate_ams
from .config import AM_REPOSITORY_FOLDER
from .validate.check_am import check_ams

_AM_BUCKET = 'am'


def _upload_to_ovh(local_filename: str, remote_filename: str) -> None:
    upload_document(_AM_BUCKET, init_swift_service(), local_filename, remote_filename)


def _remote_filename() -> str:
    return f'{datetime.now().isoformat()}.zip'


def load_ams_in_ovh() -> None:
    generate_ams()
    generate_am_repository()
    check_ams()
    with tempfile.NamedTemporaryFile('w', prefix='am-repo') as file_:
        shutil.make_archive(file_.name, 'zip', AM_REPOSITORY_FOLDER)
        _upload_to_ovh(file_.name, _remote_filename())


if __name__ == '__main__':
    load_ams_in_ovh()
