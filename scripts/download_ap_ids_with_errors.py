'''Download ids of APs for which OCR did not work.'''
import re
import shutil
import tempfile
from typing import List, Literal, Set, Tuple, TypeVar

import requests
from swiftclient.service import SwiftService

from tasks.common.ovh_upload import BucketName, init_swift_service

T = TypeVar('T')


_GEORISQUES_ID_REGEXP = re.compile(r'^[A-Z]{1}/[a-f0-9]{1}/[a-f0-9]{32}\.')
_BUCKET_URL = 'https://storage.sbg.cloud.ovh.net/v1/AUTH_3287ea227a904f04ad4e8bceb0776108/ap'

_OCRStatus = Literal['error', 'success']


def download_document(url: str, output_filename: str) -> None:
    req = requests.get(url, stream=True)
    if req.status_code == 200:
        with open(output_filename, 'wb') as f:
            req.raw.decode_content = True
            shutil.copyfileobj(req.raw, f)
    else:
        raise ValueError(f'Error when downloading document: {req.content.decode()}')


def _get_bucket_object_names(bucket: BucketName, service: SwiftService) -> List[str]:
    lists = list(service.list(bucket))
    return [x['name'] for list_ in lists for x in list_['listing']]


def _get_uploaded_ap_files() -> List[str]:
    return _get_bucket_object_names('ap', init_swift_service())


def _ovh_error_filename(georisques_id: str) -> str:
    return f'{georisques_id}.error.txt'


def _extract_status(file_extension: str) -> _OCRStatus:
    if file_extension == 'pdf':
        return 'success'
    if file_extension == 'error.txt':
        return 'error'
    raise ValueError(f'Unexpected file extension {file_extension}')


def _extract_id_and_status(filename: str) -> Tuple[str, _OCRStatus]:
    assert re.match(_GEORISQUES_ID_REGEXP, filename), f'filename {filename} does not contain id.'
    georisques_id, *extension = filename.split('.')
    try:
        return georisques_id, _extract_status('.'.join(extension))
    except ValueError:
        print(f'Filename: {filename}')
        raise


def _extract_ids_and_statuses(filenames: Set[str]) -> Set[Tuple[str, _OCRStatus]]:
    return {_extract_id_and_status(filename) for filename in filenames if re.match(_GEORISQUES_ID_REGEXP, filename)}


def _fetch_already_processed_ids_with_statuses() -> Set[Tuple[str, _OCRStatus]]:
    remote_filenames = set(_get_uploaded_ap_files())
    return _extract_ids_and_statuses(remote_filenames)


def _fetch_errors() -> Set[str]:
    ids_with_statuses = _fetch_already_processed_ids_with_statuses()
    return {id_ for id_, status in ids_with_statuses if status == 'error'}


def _read_remote_file(remote_filename: str) -> str:
    with tempfile.NamedTemporaryFile() as file_:
        download_document(f'{_BUCKET_URL}/{remote_filename}', file_.name)
        return file_.read().decode()


def run():
    errors = _fetch_errors()
    error_filenames = sorted({_ovh_error_filename(id_) for id_ in errors})
    print(f'Found {len(errors)} errors')
    print(error_filenames)


if __name__ == '__main__':
    run()
