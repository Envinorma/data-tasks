import argparse
import json
import random
import re
import shutil
import tempfile
import time
import traceback
from datetime import datetime
from typing import Iterable, List, Literal, Optional, Set, Tuple, TypeVar

import requests
from ocrmypdf import Verbosity, configure_logging, ocr
from ocrmypdf.exceptions import PriorOcrFoundError
from tqdm import tqdm

from tasks.common.ovh import OVHClient

T = TypeVar('T')


def typed_tqdm(
    collection: Iterable[T], desc: Optional[str] = None, leave: bool = True, disable: bool = False
) -> Iterable[T]:
    return tqdm(collection, desc=desc, leave=leave, disable=disable)


GEORISQUES_DOWNLOAD_URL = 'http://documents.installationsclassees.developpement-durable.gouv.fr/commun'
_IDS_URL = 'https://storage.sbg.cloud.ovh.net/v1/AUTH_3287ea227a904f04ad4e8bceb0776108/ap/georisques_ids.json'
configure_logging(Verbosity.quiet)


def download_document(url: str, output_filename: str) -> None:
    req = requests.get(url, stream=True)
    if req.status_code == 200:
        with open(output_filename, 'wb') as f:
            req.raw.decode_content = True
            shutil.copyfileobj(req.raw, f)
    else:
        raise ValueError(f'Error when downloading document: {req.content.decode()}')


def _load_all_georisques_ids() -> List[str]:
    with tempfile.TemporaryFile('w') as file_:
        download_document(_IDS_URL, file_.name)
        return json.load(file_)


def _url(georisques_id: str) -> str:
    return f'{GEORISQUES_DOWNLOAD_URL}/{georisques_id}.pdf'


def _ocr(input_filename: str, output_filename: str) -> None:
    try:
        ocr(input_filename, output_filename, language=['fra'], progress_bar=False, jobs=1)  # type: ignore
    except PriorOcrFoundError:
        pass  # no work to do


def _upload_to_ovh(filename: str, destination: str) -> None:
    OVHClient.upload_document('ap', filename, destination)


def _ovh_filename(georisques_id: str) -> str:
    return f'{georisques_id}.pdf'


def _ovh_error_filename(georisques_id: str) -> str:
    return f'{georisques_id}.error.txt'


def _download_ocr_and_upload_document(georisques_id: str):
    with tempfile.NamedTemporaryFile() as file_:
        download_document(_url(georisques_id), file_.name)
        _ocr(file_.name, file_.name)
        _upload_to_ovh(file_.name, _ovh_filename(georisques_id))


def _upload_error_file(georisques_id: str, error: str):
    with tempfile.NamedTemporaryFile(mode='w') as file_:
        file_.write(error)
        file_.flush()
        _upload_to_ovh(file_.name, _ovh_error_filename(georisques_id))


def _file_already_processed(georisques_id: str) -> bool:
    ocred_file_exists = OVHClient.file_exists(_ovh_filename(georisques_id), 'ap')
    error_file_exists = OVHClient.file_exists(_ovh_error_filename(georisques_id), 'ap')
    return ocred_file_exists or error_file_exists


def _get_uploaded_ap_files() -> List[str]:
    return OVHClient.list_bucket_object_names('ap')


def _get_computed_nb_tasks() -> int:
    ids_with_statuses = _fetch_already_processed_ids_with_statuses()
    error_ids = {id_ for id_, status in ids_with_statuses if status == 'error'}
    success_ids = {id_ for id_, status in ids_with_statuses if status == 'success'}
    all_ids = set(_load_all_georisques_ids())
    remaining = all_ids - success_ids - error_ids
    return len(all_ids) - len(remaining)


def _eta_to_days_hours_minutes(eta: float) -> Tuple[int, int, int]:
    minutes = (eta // 60) % 60
    hours = (eta // 3600) % 24
    days = eta // 86400
    return int(days), int(hours), int(minutes)


def _print_advancement(datetimes: List[datetime], all_nb_computed_tasks: List[int], total_nb_tasks: int) -> None:
    current_datetime = datetimes[-1]
    current_nb_computed_tasks = all_nb_computed_tasks[-1]
    remaining_nb_tasks = total_nb_tasks - current_nb_computed_tasks
    print(f'ETA estimations at {current_datetime}:')
    for datetime_, nb_computed_tasks in zip(datetimes[-4:-1], all_nb_computed_tasks[-4:-1]):
        elapsed_time = (current_datetime - datetime_).total_seconds()
        nb_computed_tasks_during_this_time = current_nb_computed_tasks - nb_computed_tasks
        eta = (elapsed_time / (nb_computed_tasks_during_this_time or 1)) * remaining_nb_tasks
        days, hours, minutes = _eta_to_days_hours_minutes(eta)
        print(
            f'Computed: {nb_computed_tasks}/{total_nb_tasks}, Remaining: ' f'{days}d {hours}h {minutes}m',
        )


def _run_compute_advancement() -> None:
    total_nb_tasks = len(set(_load_all_georisques_ids()))
    all_computed_nb_tasks: List[int] = []
    datetimes: List[datetime] = []
    sleep_times = [30, 60, 5 * 60]
    epoch = 0
    while True:
        all_computed_nb_tasks.append(_get_computed_nb_tasks())
        datetimes.append(datetime.now())
        if epoch > 0:
            _print_advancement(datetimes, all_computed_nb_tasks, total_nb_tasks)
        sleep_time = sleep_times[epoch] if epoch < len(sleep_times) else sleep_times[-1]
        epoch += 1
        time.sleep(sleep_time)


_GEORISQUES_ID_REGEXP = re.compile(r'^[A-Z]{1}/[a-f0-9]{1}/[a-f0-9]{32}\.')

_OCRStatus = Literal['error', 'success']


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


def _fetch_already_processed_ids() -> Set[str]:
    return {id_ for id_, _ in _fetch_already_processed_ids_with_statuses()}


def _load_remaining_ids() -> List[str]:
    already_processed_ids = _fetch_already_processed_ids()
    ids_to_process = set(_load_all_georisques_ids())
    return list(ids_to_process - already_processed_ids)


def _run_ocr(force_redo_ocr: bool) -> None:
    ids = _load_remaining_ids()
    random.shuffle(ids)

    for id_ in typed_tqdm(ids):
        if _file_already_processed(id_) and not force_redo_ocr:
            continue
        try:
            _download_ocr_and_upload_document(id_)
        except Exception:
            error = traceback.format_exc()
            _upload_error_file(id_, error)
            print(f'Error when processing {id_}:\n{error}')


def _run(compute_advancement: bool = False, force_redo_ocr: bool = False) -> None:
    if compute_advancement:
        _run_compute_advancement()
    else:
        _run_ocr(force_redo_ocr)


def cli() -> None:
    parser = argparse.ArgumentParser(description='Run the OCR pipeline')
    parser.add_argument(
        '--compute-advancement', action='store_true', help='Run computation of advancement', required=False
    )
    parser.add_argument(
        '--force-redo-ocr',
        action='store_true',
        help='Force redo OCR even if the file is already processed',
        required=False,
    )
    args = parser.parse_args()
    _run(compute_advancement=args.compute_advancement, force_redo_ocr=args.force_redo_ocr)


if __name__ == '__main__':
    cli()
