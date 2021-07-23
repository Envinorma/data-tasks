import argparse
import json
import pathlib
import random
import re
import shutil
import tempfile
import time
import traceback
from datetime import datetime
from typing import Dict, Iterable, List, Literal, Optional, Set, Tuple, TypeVar

import requests
from ocrmypdf import Verbosity, configure_logging, ocr
from ocrmypdf.exceptions import PriorOcrFoundError
from swiftclient.service import SwiftService
from tqdm import tqdm

from tasks.common.ovh_upload import BucketName, init_swift_service, upload_document

T = TypeVar('T')


def typed_tqdm(
    collection: Iterable[T], desc: Optional[str] = None, leave: bool = True, disable: bool = False
) -> Iterable[T]:
    return tqdm(collection, desc=desc, leave=leave, disable=disable)


def _data_filename() -> str:
    candidate = pathlib.Path(__file__).parent / 'georisques_ids.json'
    if not candidate.exists():
        raise ValueError('Data file not found.')
    return str(candidate)


GEORISQUES_DOWNLOAD_URL = 'http://documents.installationsclassees.developpement-durable.gouv.fr/commun'
configure_logging(Verbosity.quiet)


def _load_all_georisques_ids() -> List[str]:
    return json.load(open(_data_filename()))


def download_document(url: str, output_filename: str) -> None:
    req = requests.get(url, stream=True)
    if req.status_code == 200:
        with open(output_filename, 'wb') as f:
            req.raw.decode_content = True
            shutil.copyfileobj(req.raw, f)
    else:
        raise ValueError(f'Error when downloading document: {req.content.decode()}')


def _url(georisques_id: str) -> str:
    return f'{GEORISQUES_DOWNLOAD_URL}/{georisques_id}.pdf'


def _ocr(input_filename: str, output_filename: str) -> None:
    try:
        ocr(input_filename, output_filename, language=['fra'], progress_bar=False, jobs=1)  # type: ignore
    except PriorOcrFoundError:
        pass  # no work to do


def _upload_to_ovh(filename: str, destination: str) -> None:
    upload_document('ap', init_swift_service(), filename, destination)


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
        _upload_to_ovh(file_.name, _ovh_error_filename(georisques_id))


def _file_exists(filename: str, bucket_name: BucketName, service: SwiftService) -> bool:
    results: List[Dict] = list(service.stat(bucket_name, [filename]))  # type: ignore
    return results[0]['success']


def _file_already_processed(georisques_id: str) -> bool:
    ocred_file_exists = _file_exists(_ovh_filename(georisques_id), 'ap', init_swift_service())
    error_file_exists = _file_exists(_ovh_error_filename(georisques_id), 'ap', init_swift_service())
    return ocred_file_exists or error_file_exists


def _get_bucket_object_names(bucket: BucketName, service: SwiftService) -> List[str]:
    lists = list(service.list(bucket))
    return [x['name'] for list_ in lists for x in list_['listing']]


def _get_uploaded_ap_files() -> List[str]:
    return _get_bucket_object_names('ap', init_swift_service())


def _get_tasks_statuses_counter() -> Dict[str, int]:
    ids_with_statuses = _fetch_already_processed_ids_with_statuses()
    error_ids = {id_ for id_, status in ids_with_statuses if status == 'error'}
    success_ids = {id_ for id_, status in ids_with_statuses if status == 'success'}
    all_ids = set(_load_all_georisques_ids())
    return {
        'success': len(success_ids - (success_ids - all_ids)),
        'error': len(error_ids - (error_ids - all_ids)),
        'total': len(all_ids),
    }


def _eta_to_days_hours_minutes(eta: float) -> Tuple[int, int, int]:
    minutes = (eta // 60) % 60
    hours = (eta // 3600) % 24
    days = eta // 86400
    return int(days), int(hours), int(minutes)


def _print_advancement(datetimes: List[datetime], status_counters: List[Dict[str, int]]) -> None:
    current_datetime = datetimes[-1]
    current_counter = status_counters[-1]
    total_nb_tasks = current_counter['total']
    current_nb_computed_tasks = current_counter['success'] + current_counter['error']
    remaining_nb_tasks = total_nb_tasks - current_nb_computed_tasks
    print('ETA estimations:')
    for datetime_, status_counter in zip(datetimes[-4:-1], status_counters[-4:-1]):
        elapsed_time = (current_datetime - datetime_).total_seconds()
        nb_computed_tasks = status_counter['success'] + status_counter['error']
        nb_computed_tasks_during_this_time = current_nb_computed_tasks - nb_computed_tasks
        eta = (elapsed_time / nb_computed_tasks_during_this_time) * remaining_nb_tasks
        days, hours, minutes = _eta_to_days_hours_minutes(eta)
        print(
            f'Computed: {nb_computed_tasks_during_this_time}/{total_nb_tasks}, Remaining: '
            f'{days}d {hours}h {minutes}m',
        )


def _run_compute_advancement() -> None:
    all_statuses: List[Dict[str, int]] = []
    datetimes: List[datetime] = []
    sleep_times = [30, 60, 5 * 60]
    epoch = 0
    while True:
        statuses = _get_tasks_statuses_counter()
        all_statuses.append(statuses)
        datetimes.append(datetime.now())
        _print_advancement(datetimes, all_statuses)
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


def _run_ocr() -> None:
    ids = _load_remaining_ids()
    random.shuffle(ids)

    for id_ in typed_tqdm(ids):
        if _file_already_processed(id_):
            continue
        try:
            _download_ocr_and_upload_document(id_)
        except Exception:
            error = traceback.format_exc()
            _upload_error_file(id_, error)
            print(f'Error when processing {id_}:\n{error}')


def run() -> None:
    parser = argparse.ArgumentParser(description='Run the OCR pipeline')
    parser.add_argument(
        '--compute-advancement', action='store_true', help='Run computation of advancement', required=False
    )
    args = parser.parse_args()
    if args.compute_advancement:
        _run_compute_advancement()
    else:
        _run_ocr()


if __name__ == '__main__':
    run()
