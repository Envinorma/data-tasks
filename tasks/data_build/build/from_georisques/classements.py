from tasks.common.ovh import dump_in_ovh
import pandas as pd
from tqdm import tqdm

from envinorma.models import DetailedClassement
from tasks.data_build.load import load_classements_csv, load_installation_ids
from tasks.data_build.filenames import Dataset, dataset_object_name


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
    name = dataset_object_name('all', 'classements')
    dump_in_ovh(name, 'misc', lambda filename: classements.to_csv(filename, index=False))
    print(f'classements dataset all has {classements.shape[0]} rows')


def _filter_and_dump(all_classements: pd.DataFrame, dataset: Dataset) -> None:
    installation_ids = load_installation_ids(dataset)
    filtered_df = all_classements[all_classements.s3ic_id.apply(lambda x: x in installation_ids)]
    nb_rows = filtered_df.shape[0]
    assert nb_rows >= 1000, f'Expecting >= 1000 classements, got {nb_rows}'
    name = dataset_object_name(dataset, 'classements')
    dump_in_ovh(name, 'misc', lambda filename: filtered_df.to_csv(filename, index=False))
    print(f'classements dataset {dataset} has {nb_rows} rows')


def build_all_classements_datasets() -> None:
    all_classements = load_classements_csv('all')
    _filter_and_dump(all_classements, 'sample')
    _filter_and_dump(all_classements, 'idf')
