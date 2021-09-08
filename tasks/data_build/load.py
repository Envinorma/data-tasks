import logging
from datetime import date
from typing import Any, Dict, List, Optional, Set, cast

import pandas
import pandas as pd
from envinorma.models import DetailedClassement, Regime
from envinorma.models.am_metadata import AMMetadata
from envinorma.models.arrete_ministeriel import ArreteMinisteriel
from envinorma.models.document import Document, DocumentType
from envinorma.models.installation import ActivityStatus, Installation, InstallationFamily, Seveso
from envinorma.utils import ensure_not_none

from tasks.common.config import DATA_FETCHER
from tasks.data_build.filenames import Dataset, dataset_filename
from tasks.data_build.utils import typed_tqdm


def load_installations_csv(dataset: Dataset) -> pd.DataFrame:
    return pd.read_csv(dataset_filename(dataset, 'installations'), dtype='str')


def load_documents_csv(dataset: Dataset) -> pd.DataFrame:
    return pd.read_csv(dataset_filename(dataset, 'documents'), dtype='str')


def _dataframe_record_to_installation(record: Dict[str, Any]) -> Installation:
    record['last_inspection'] = date.fromisoformat(record['last_inspection']) if record['last_inspection'] else None
    record['regime'] = Regime(record['regime'])
    record['seveso'] = Seveso(record['seveso'])
    record['family'] = InstallationFamily(record['family'])
    record['active'] = ActivityStatus(record['family'])
    return Installation(**record)


def load_installations(dataset: Dataset) -> List[Installation]:
    filename = dataset_filename(dataset, 'installations')
    dataframe = pandas.read_csv(filename, dtype='str', na_values=None).fillna('')
    return [
        _dataframe_record_to_installation(cast(Dict, record))
        for record in typed_tqdm(dataframe.to_dict(orient='records'), 'Loading installations', leave=False)
    ]


def load_installation_ids(dataset: Dataset = 'all') -> Set[str]:
    return {x for x in load_installations_csv(dataset).s3ic_id}


def _dataframe_record_to_classement(record: Dict[str, Any]) -> DetailedClassement:
    return DetailedClassement(**record)


def load_classements(dataset: Dataset) -> List[DetailedClassement]:
    filename = dataset_filename(dataset, 'classements')
    dataframe_with_nan = pandas.read_csv(filename, dtype='str', na_values=None)
    dataframe = dataframe_with_nan.where(pandas.notnull(dataframe_with_nan), None)
    dataframe['volume'] = dataframe.volume.apply(lambda x: x or '')
    return [
        _dataframe_record_to_classement(cast(Dict, record))
        for record in typed_tqdm(dataframe.to_dict(orient='records'), 'Loading classements', leave=False)
    ]


def load_documents_from_csv(dataset: Dataset) -> List[Document]:
    return [
        Document.from_dict(doc)
        for doc in pd.read_csv(dataset_filename(dataset, 'documents'), dtype='str').fillna('').to_dict(orient='records')
    ]


def _dataframe_record_to_ap(record: Dict[str, Any]) -> Document:
    record = record.copy()
    record['s3ic_id'] = record['installation_s3ic_id']
    record['url'] = record['georisques_id'] + '.pdf'
    record['type'] = DocumentType.AP.value
    record['date'] = record['date'] if isinstance(record['date'], str) else None
    del record['installation_s3ic_id']
    del record['georisques_id']
    return Document.from_dict(record)


def load_aps(dataset: Dataset) -> List[Document]:
    dataframe = pandas.read_csv(dataset_filename(dataset, 'aps'), dtype='str')
    return [
        _dataframe_record_to_ap(record)
        for record in typed_tqdm(dataframe.to_dict(orient='records'), 'Loading aps', leave=False)
    ]


def load_am_metadata() -> Dict[str, AMMetadata]:
    return {id_: md for id_, md in DATA_FETCHER.load_all_am_metadata().items() if not id_.startswith('FAKE')}


def load_ams(ids: Optional[Set[str]] = None) -> Dict[str, ArreteMinisteriel]:
    """Load all ams or specify a set of am ids to load.

    Args:
        ids (Optional[Set[str]], optional): Set of ids to load. Defaults to None.

    Returns:
        Dict[str, ArreteMinisteriel]: Dict mapping am_id to the corresponding AM.
    """
    logging.info('loading AM.')
    ids = ids or set(list(DATA_FETCHER.load_all_am_metadata().keys()))
    ams = DATA_FETCHER.load_id_to_most_advanced_am(ids)
    return {id_: ensure_not_none(ams.get(id_)) for id_ in ids}
