'''Generate installations datasets from georisques data.'''
import os
from dataclasses import fields
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

import pandas as pd
from envinorma.models import Regime
from envinorma.models.installation import ActivityStatus, Installation, InstallationFamily, Seveso

from tasks.data_build.config import GEORISQUES_DATA_FOLDER
from tasks.data_build.filenames import Dataset, dataset_filename
from tasks.data_build.load import load_installations_csv

_COLS = [
    'code_s3ic',
    'num_siret',
    'x',
    'y',
    'epsg',
    'raison_sociale',
    'insee_code',
    'code_postal_cedex',
    'valeur_etat_activite_id',
    'code_activite_naf',
    'nomcommune',
    'seveso',
    'regime',
    'priorite_nationale',
    'ippc',
    'declaration_annuelle_emission',
    'famille_ic',
    'base_id_service',
    'nature_id_service',
    'adressepartie1',
    'adressepartie2',
    'date_inspection',
    'indication_ssp',
    'rayon',
    'precision_positionnement',
]


def _load_georisques_installations() -> pd.DataFrame:
    input_filename = os.path.join(GEORISQUES_DATA_FOLDER, 'IC_etablissement.csv')
    result = pd.read_csv(input_filename, sep=';', header=None, names=_COLS, dtype='str')  # type: ignore
    return result


def _extract_num_dep(code_insee: Union[int, Optional[str]]) -> str:
    if isinstance(code_insee, (float, int)):
        code_insee = str(code_insee)
    if not code_insee:
        return ''
    if code_insee.startswith('97'):
        return code_insee[:3]
    return code_insee[:2]


def _load_geomapping() -> Dict[str, Tuple[str, str]]:
    mapping_dataframe = pd.read_csv(Path(__file__).parent / 'geomapping.csv')
    return {
        str(row['num_dep']): (str(row['region']), str(row['department'])) for _, row in mapping_dataframe.iterrows()
    }


def _extract_family(family_code: str) -> str:
    if family_code == 'IN':
        return InstallationFamily.INDUSTRIES.value
    if family_code == 'PO':
        return InstallationFamily.PORCS.value
    if family_code == 'BO':
        return InstallationFamily.BOVINS.value
    if family_code == 'VO':
        return InstallationFamily.VOLAILLES.value
    if family_code == 'CA':
        return InstallationFamily.CARRIERES.value
    raise ValueError(f'Unknown family code: {family_code}')


def _extract_activity_status(activity_status_code: str) -> str:
    if activity_status_code == '2':
        return ActivityStatus.EN_FONCTIONNEMENT.value
    if activity_status_code == '1':
        return ActivityStatus.EN_CONSTRUCTION.value
    if activity_status_code == '3':
        return ActivityStatus.A_L_ARRET.value
    if activity_status_code == '4':
        return ActivityStatus.CESSATION_DECLAREE.value
    if activity_status_code == '5':
        return ActivityStatus.RECOLEMENT_FAIT.value
    return ActivityStatus.EMPTY.value


def _extract_regime(regime_code: Union[str, float]) -> Optional[str]:
    if regime_code in ('A', 'E', 'NC'):
        return Regime(regime_code).value
    if regime_code in ('D', 'DC'):
        return Regime.D.value
    return None


def _extract_seveso(seveso_code: Union[str, float]) -> Optional[str]:
    if isinstance(seveso_code, float):
        return None
    return Seveso(seveso_code).value


def _convert_to_envinorma_installations(installations: pd.DataFrame) -> pd.DataFrame:
    installations = installations.copy()
    installations['s3ic_id'] = installations.code_s3ic
    installations['num_dep'] = installations.insee_code.apply(_extract_num_dep)
    geomapping = _load_geomapping()
    installations['region'] = installations.num_dep.apply(lambda x: geomapping[x][0])
    installations['department'] = installations.num_dep.apply(lambda x: geomapping[x][1])
    installations['city'] = installations.nomcommune
    installations['name'] = installations.raison_sociale
    installations['lat'] = installations.x
    installations['lon'] = installations.y
    installations['last_inspection'] = installations.date_inspection
    installations['regime'] = installations['regime'].apply(_extract_regime)
    installations['seveso'] = installations['seveso'].apply(_extract_seveso)
    installations['active'] = installations['valeur_etat_activite_id'].apply(_extract_activity_status)
    installations['family'] = installations['famille_ic'].apply(_extract_family)
    installations['code_insee'] = installations.insee_code
    installations['code_postal'] = installations.code_postal_cedex
    installations['code_naf'] = installations.code_activite_naf
    cols = [field.name for field in fields(Installation)]
    return installations[cols]


def build_all_installations() -> None:
    georisques_installations = _load_georisques_installations()
    envinorma_installations = _convert_to_envinorma_installations(georisques_installations)
    envinorma_installations.sort_values(by='s3ic_id').to_csv(dataset_filename('all', 'installations'), index=False)
    print(f'Dumped {envinorma_installations.shape[0]} installations.')


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
    filtered_df.to_csv(dataset_filename(dataset, 'installations'), index=False)


def build_all_installations_datasets() -> None:
    all_installations = load_installations_csv('all')
    _filter_and_dump(all_installations, 'sample')
    _filter_and_dump(all_installations, 'idf')
