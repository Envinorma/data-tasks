from datetime import date
from tasks.common.ovh import dump_in_ovh
from tasks.data_build.load import load_installations_csv
from typing import Any, Dict, cast

import pandas as pd
from envinorma.models import Regime
from envinorma.models.installation import ActivityStatus, Installation, InstallationFamily, Seveso
from tqdm import tqdm

from tasks.data_build.filenames import S3IC_INSTALLATIONS_FILENAME, Dataset, dataset_object_name


def _load_A_E_installations() -> pd.DataFrame:
    raw_installations = pd.read_csv(S3IC_INSTALLATIONS_FILENAME, sep=';', dtype='str')
    without_duplicates = raw_installations.drop_duplicates()
    publishable_regimes = {'A', 'E', 'S', '2'}
    regime_key = 'régime_etab_en_vigueur'
    without_D = without_duplicates[without_duplicates[regime_key].apply(lambda x: x in publishable_regimes)].copy()
    regime_map = {'A': 'A', 'E': 'E', 'S': 'A', '2': 'A'}
    without_D.loc[:, regime_key] = without_D[regime_key].apply(lambda x: regime_map[x])  # type: ignore
    return without_D.groupby('code_s3ic').last().reset_index()  # keep one installation per code_s3ic


def _rename_installations_columns(input_installations: pd.DataFrame) -> pd.DataFrame:
    column_mapping = {
        'code_s3ic': 's3ic_id',
        'region': 'region',
        'département': 'department',
        'commune_principale': 'city',
        'raison_sociale': 'name',
        'coordonnées_géographiques_x': 'lat',
        'coordonnées_géographiques_y': 'lon',
        'date_inspection': 'last_inspection',
        'régime_etab_en_vigueur': 'regime',
        'statut_seveso': 'seveso',
        'famille': 'family',
        'état_de_l_activité': 'active',
        'code_postal': 'code_postal',
        'code_insee_commune': 'code_insee',
        'code_naf': 'code_naf',
    }
    return input_installations.rename(columns=cast(Any, column_mapping))


def _map_family(family_in: str) -> str:
    if family_in == 'industrie':
        return InstallationFamily.INDUSTRIES.value
    if family_in == 'carriere':
        return InstallationFamily.CARRIERES.value
    if family_in == 'volailles':
        return InstallationFamily.VOLAILLES.value
    if family_in == 'bovins':
        return InstallationFamily.BOVINS.value
    if family_in == 'porcs':
        return InstallationFamily.PORCS.value
    return family_in


def _map_seveso(seveso_in: str) -> str:
    if seveso_in == 'SSH':
        return Seveso.SEUIL_HAUT.value
    if seveso_in == 'SSB':
        return Seveso.SEUIL_BAS.value
    return seveso_in


def _modify_and_keep_final_installations_cols(installations: pd.DataFrame) -> pd.DataFrame:
    installations = installations.copy()
    installations.loc[:, 'num_dep'] = installations.code_postal.apply(lambda x: (x or '')[:2])  # type: ignore
    installations.loc[:, 'last_inspection'] = installations.last_inspection.apply(  # type: ignore
        lambda x: date.fromisoformat(x) if isinstance(x, str) else None
    )
    installations.loc[:, 'family'] = installations.family.apply(_map_family)  # type: ignore
    installations.loc[:, 'active'] = installations['active'].fillna('')  # type: ignore
    installations.loc[:, 'seveso'] = installations['seveso'].fillna('').apply(_map_seveso)  # type: ignore
    expected_keys = [x for x in Installation.__dataclass_fields__]  # type: ignore
    return cast(pd.DataFrame, installations[expected_keys])


def _dataframe_record_to_installation(record: Dict[str, Any]) -> Installation:
    record['regime'] = Regime(record['regime'])
    record['seveso'] = Seveso(record['seveso'])
    record['family'] = InstallationFamily(record['family'])
    record['active'] = ActivityStatus(record['active'])
    return Installation(**record)


def _check_installations(installations: pd.DataFrame) -> None:
    for record in tqdm(installations.to_dict(orient='records')):
        _dataframe_record_to_installation(record)


_ACCEPTED_STATUSES = {ActivityStatus.EN_FONCTIONNEMENT.value, ActivityStatus.EN_CONSTRUCTION.value}


def _active_or_in_construction(activity_status: str) -> bool:
    return activity_status in _ACCEPTED_STATUSES


def build_installations_csv() -> None:
    A_E_installations = _load_A_E_installations()
    installations_with_renamed_columns = _rename_installations_columns(A_E_installations)
    final_installations = _modify_and_keep_final_installations_cols(installations_with_renamed_columns)
    _check_installations(final_installations)
    final_active_installations = final_installations[final_installations.active.apply(_active_or_in_construction)]
    name = dataset_object_name('all', 'installations')
    dump_in_ovh(name, 'misc', lambda filename: final_active_installations.to_csv(filename, index=False))
    print(f'Dumped {final_active_installations.shape[0]} active installations.')


def _select(s3ic_id: str) -> bool:
    return sum([ord(x) for x in s3ic_id]) % 10 == 0  # proba 1/10


def _filter_and_dump(all_installations: pd.DataFrame, dataset: Dataset) -> None:
    if dataset == 'idf':
        filtered_df = all_installations[all_installations.region == 'ILE DE FRANCE']
    elif dataset == 'sample':
        filtered_df = all_installations[all_installations.s3ic_id.apply(_select)]
    else:
        raise NotImplementedError(dataset)
    nb_rows = filtered_df.shape[0]
    assert nb_rows >= 1000, f'Expecting >= 1000 installations, got {nb_rows}'
    print(f'Installation dataset {dataset} has {nb_rows} rows')
    name = dataset_object_name(dataset, 'installations')
    dump_in_ovh(name, 'misc', lambda filename: filtered_df.to_csv(filename, index=False))


def build_all_installations_datasets() -> None:
    all_installations = load_installations_csv('all')
    _filter_and_dump(all_installations, 'sample')
    _filter_and_dump(all_installations, 'idf')
