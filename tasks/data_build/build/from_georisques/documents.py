import tarfile
import os
from dataclasses import fields
from typing import cast

import pandas as pd
from envinorma.models.document import Document

from tasks.common.ovh import dump_in_ovh
from tasks.common import download_document
from tasks.data_build.config import GEORISQUES_DATA_FOLDER, GEORISQUES_DUMP_URL
from tasks.data_build.filenames import Dataset, dataset_object_name
from tasks.data_build.load import load_documents_csv, load_installation_ids

_COLS = ['id_document', 'code_s3ic', 'type_id_document', 'nom', 'url_document', 'date_document']


def _download_and_extract_zip() -> None:
    print('Downloading Georisques zip file...')
    zip_file = os.path.join(GEORISQUES_DATA_FOLDER, 'georisques_data.tar.gz')
    download_document(GEORISQUES_DUMP_URL, zip_file)
    with tarfile.open(zip_file, 'r:gz') as zip_ref:
        zip_ref.extractall(GEORISQUES_DATA_FOLDER)


def _load_georisques_documents() -> pd.DataFrame:
    input_filename = os.path.join(GEORISQUES_DATA_FOLDER, 'IC_documents.csv')
    result = pd.read_csv(input_filename, sep=';', header=None, names=_COLS, dtype='str')  # type: ignore
    return result


def _load_type_mapping() -> pd.DataFrame:
    input_filename = os.path.join(GEORISQUES_DATA_FOLDER, 'IC_types_document.csv')
    return pd.read_csv(input_filename, sep=';', header=None, names=['id', 'type'], dtype='str')  # type: ignore


def _convert_to_envinorma_format(georisques_documents: pd.DataFrame) -> pd.DataFrame:
    georisques_documents = georisques_documents.copy()
    type_mapping = _load_type_mapping()
    doc_with_types = pd.merge(georisques_documents, type_mapping, left_on='type_id_document', right_on='id', how='left')
    doc_with_types['s3ic_id'] = doc_with_types['code_s3ic']
    doc_with_types['date'] = doc_with_types['date_document']
    doc_with_types['description'] = doc_with_types['nom']
    doc_with_types['url'] = doc_with_types['url_document']
    cols = [x.name for x in fields(Document)]
    return doc_with_types[cols]


def build_all_documents() -> None:
    _download_and_extract_zip()
    georisques_documents = _load_georisques_documents()
    envinorma_documents = _convert_to_envinorma_format(georisques_documents)
    sorted_documents = envinorma_documents.sort_values(by='s3ic_id')
    dump_in_ovh(
        dataset_object_name('all', 'documents'), 'misc', lambda filename: sorted_documents.to_csv(filename, index=False)
    )
    print(f'Dumped {envinorma_documents.shape[0]} documents.')


def _filter_and_dump(all_documents: pd.DataFrame, dataset: Dataset) -> None:
    doc_ids = load_installation_ids(dataset)
    filtered_documents = cast(pd.DataFrame, all_documents[all_documents.s3ic_id.apply(lambda x: x in doc_ids)])
    sort_keys = ['s3ic_id', 'date', 'url']
    sorted_documents = filtered_documents.sort_values(by=sort_keys, ascending=[True, False, True])
    dump_in_ovh(
        dataset_object_name(dataset, 'documents'),
        'misc',
        lambda filename: sorted_documents.to_csv(filename, index=False),
    )
    print(f'documents dataset {dataset} has {len(sorted_documents)} rows')
    assert len(sorted_documents) >= 100, f'Expecting >= 100 docs, got {len(sorted_documents)}'


def build_all_documents_datasets() -> None:
    all_documents = load_documents_csv('all')
    _filter_and_dump(all_documents, 'all')
    _filter_and_dump(all_documents, 'sample')
    _filter_and_dump(all_documents, 'idf')
