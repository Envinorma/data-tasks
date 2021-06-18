def _set_environment_variables() -> None:
    # To keep above OVH import to ensure env vars are set correctly
    from ..common.config import PSQL_DSN  # noqa: F401


_set_environment_variables()

import shutil  # noqa: E402
import tempfile  # noqa: E402
from datetime import datetime  # noqa: E402

from ..common.ovh_upload import BucketName, init_swift_service, upload_document  # noqa: E402
from .build.build_am_repository import generate_am_repository  # noqa: E402
from .build.build_ams import generate_ams  # noqa: E402
from .config import AM_REPOSITORY_FOLDER  # noqa: E402
from .validate.check_am import check_ams  # noqa: E402

_AM_BUCKET: BucketName = 'am'


def _upload_to_ovh(local_filename: str, remote_filename: str) -> None:
    upload_document(_AM_BUCKET, init_swift_service(), local_filename, remote_filename)


def _remote_filename() -> str:
    return f'data/{datetime.now().isoformat()}.zip'


def load_ams_in_ovh() -> None:
    generate_ams()
    generate_am_repository()
    check_ams()
    with tempfile.NamedTemporaryFile('w', prefix='am-repo') as file_:
        shutil.make_archive(file_.name, 'zip', AM_REPOSITORY_FOLDER)
        _upload_to_ovh(f'{file_.name}.zip', _remote_filename())


if __name__ == '__main__':
    load_ams_in_ovh()
