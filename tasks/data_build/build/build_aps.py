import json
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

import pandas
from envinorma.models.document import Document, DocumentType

from tasks.common.ovh import OVHClient, dump_in_ovh, load_from_ovh
from tasks.data_build.filenames import Dataset, dataset_object_name
from tasks.data_build.load import load_documents_from_csv

OCRStatus = Literal['ERROR', 'SUCCESS', 'NOT_ATTEMPTED']


def _rowify_ap(ap: Document, status: OCRStatus, document_size: Optional[int]) -> Dict[str, Any]:
    assert ap.type == ap.type.AP
    return {
        'installation_s3ic_id': ap.s3ic_id,
        'description': ap.description,
        'date': ap.date,
        'georisques_id': ap.georisques_id,
        'ocr_status': status,
        'size': document_size,
    }


def _build_aps_dataframe(aps: List[Document]) -> pandas.DataFrame:
    ap_ids = [ap.georisques_id for ap in aps]
    status_and_size = _fetch_ap_status_and_size(ap_ids)
    return pandas.DataFrame([_rowify_ap(ap, *status_and_size[ap.georisques_id]) for ap in aps])


def _deduce_status_and_size(size: Optional[int], error: bool) -> Tuple[OCRStatus, Optional[int]]:
    if size is None:
        return ('ERROR' if error else 'NOT_ATTEMPTED', None)
    return ('SUCCESS', size)


def _fetch_ap_status_and_size(ap_ids: List[str]) -> Dict[str, Tuple[OCRStatus, Optional[int]]]:
    names_and_sizes = OVHClient.objects_name_and_sizes('ap')
    pdfs = {name.split('.')[0]: size for name, size in names_and_sizes.items() if name.endswith('pdf')}
    errors = {name.split('.')[0] for name in names_and_sizes if name.endswith('error.txt')}
    return {ap_id: _deduce_status_and_size(pdfs.get(ap_id), ap_id in errors) for ap_id in ap_ids}


def _ap_dumper(dataframe: pandas.DataFrame) -> Callable[[str], None]:
    def _dump(filename: str) -> None:
        dataframe.to_csv(filename, index=False)

    return _dump


def dump_aps(dataset: Dataset) -> None:
    aps = [doc for doc in load_documents_from_csv(dataset) if doc.type == DocumentType.AP]
    print(f'Found {len(aps)} AP for dataset {dataset}.')
    assert len(aps) >= 100, f'Expecting >= 100 aps, got {len(aps)}'
    dataframe = _build_aps_dataframe(aps)
    print(f'Statuses of OCR:\n{dataframe.ocr_status.value_counts()}', end='\n\n')
    dump_in_ovh(dataset_object_name(dataset, 'aps'), 'misc', _ap_dumper(dataframe))


def _ap_loader(filename: str) -> pandas.DataFrame:
    return pandas.read_csv(filename)


def _load_dataset(dataset: Dataset) -> pandas.DataFrame:
    return load_from_ovh(dataset_object_name(dataset, 'aps'), 'misc', _ap_loader)


def _ids_dumper(ids: List[str]) -> Callable[[str], None]:
    def _dump(filename: str) -> None:
        with open(filename, 'w') as file_:
            json.dump(ids, file_)

    return _dump


def _upload_georisques_ids():
    print('Uploading file IDs to OVH in preparation to OCR.')
    ids = _load_dataset('all')['georisques_id'].tolist()
    dump_in_ovh('georisques_ids.json', 'misc', _ids_dumper(ids))


def dump_ap_datasets() -> None:
    dump_aps('all')
    dump_aps('idf')
    dump_aps('sample')
    _upload_georisques_ids()
