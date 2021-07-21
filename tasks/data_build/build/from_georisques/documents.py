import os
from dataclasses import fields

import pandas as pd
from envinorma.models.document import Document

from tasks.data_build.config import GEORISQUES_DATA_FOLDER
from tasks.data_build.filenames import Dataset, dataset_filename
from tasks.data_build.load import load_documents_csv, load_installation_ids


_COLS = ['id_document', 'code_s3ic', 'type_id_document', 'nom', 'url_document', 'date_document']


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
    georisques_documents = _load_georisques_documents()
    envinorma_documents = _convert_to_envinorma_format(georisques_documents)
    envinorma_documents.sort_values(by='s3ic_id').to_csv(dataset_filename('all', 'documents'), index=False)
    print(f'Dumped {envinorma_documents.shape[0]} documents.')


def _filter_and_dump(all_documents: pd.DataFrame, dataset: Dataset) -> None:
    doc_ids = load_installation_ids(dataset)
    filtered_documents = all_documents[all_documents.s3ic_id.apply(lambda x: x in doc_ids)]
    filtered_documents.to_csv(dataset_filename(dataset, 'documents'), index=False)
    print(f'documents dataset {dataset} has {len(filtered_documents)} rows')
    assert len(filtered_documents) >= 100, f'Expecting >= 100 docs, got {len(filtered_documents)}'


def build_all_documents_datasets() -> None:
    all_documents = load_documents_csv('all')
    _filter_and_dump(all_documents, 'sample')
    _filter_and_dump(all_documents, 'idf')
