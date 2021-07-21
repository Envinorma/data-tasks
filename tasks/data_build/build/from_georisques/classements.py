from datetime import date
from typing import Any, Set, cast

import pandas as pd
from tqdm import tqdm

from envinorma.models import DetailedClassement, DetailedRegime, DetailedClassementState
from tasks.data_build.load import load_installation_ids
from tasks.data_build.filenames import DGPR_RUBRIQUES_FILENAME, Dataset, dataset_filename


def _simplify_regime(regime: str) -> str:
    return DetailedRegime(regime).to_simple_regime()


def _check_classements(classements: pd.DataFrame) -> None:
    records = classements.to_dict(orient='records')
    for rubrique in tqdm(records, 'Checking classements'):
        DetailedClassement(**rubrique)


def _build_csv() -> pd.DataFrame:
    raise NotImplementedError()


def build_all_classements() -> None:
    all_classements = load_classements_csv('all')
    _filter_and_dump(all_classements, 'all')


def build_classements_csv() -> None:
    classements = _build_csv()
    _check_classements(classements)
    classements.to_csv(dataset_filename('all', 'classements'), index=False)
    print(f'classements dataset all has {classements.shape[0]} rows')


def load_classements_csv(dataset: Dataset) -> pd.DataFrame:
    return pd.read_csv(dataset_filename(dataset, 'classements'), dtype='str')


def _filter_and_dump(all_classements: pd.DataFrame, dataset: Dataset) -> None:
    installation_ids = load_installation_ids(dataset)
    filtered_df = all_classements[all_classements.s3ic_id.apply(lambda x: x in installation_ids)]
    nb_rows = filtered_df.shape[0]
    assert nb_rows >= 1000, f'Expecting >= 1000 classements, got {nb_rows}'
    filtered_df.to_csv(dataset_filename(dataset, 'classements'), index=False)
    print(f'classements dataset {dataset} has {nb_rows} rows')


def build_all_classements_datasets() -> None:
    all_classements = load_classements_csv('all')
    _filter_and_dump(all_classements, 'sample')
    _filter_and_dump(all_classements, 'idf')
