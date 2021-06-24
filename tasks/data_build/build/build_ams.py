import json
import os
from typing import Any, Dict, List, Optional

from envinorma.models import AMMetadata, ArreteMinisteriel
from envinorma.parametrization import Parametrization
from envinorma.parametrization.am_with_versions import AMVersions, generate_am_with_versions
from envinorma.enriching import enrich
from envinorma.utils import AMStatus, ensure_not_none, typed_tqdm, write_json
from tqdm import tqdm

from tasks.data_build.config import DATA_FETCHER, generate_parametric_descriptor
from tasks.data_build.filenames import ENRICHED_OUTPUT_FOLDER
from tasks.data_build.load import load_ams

_AM_ID_TO_METADATA = {id_: md for id_, md in DATA_FETCHER.load_all_am_metadata().items() if not id_.startswith('FAKE')}


def _dump_am_versions(am_id: str, versions: AMVersions) -> None:
    for version_desc, version in versions.items():
        filename = am_id + '_' + generate_parametric_descriptor(version_desc) + '.json'
        full_path = os.path.join(ENRICHED_OUTPUT_FOLDER, filename)
        write_json(version.to_dict(), full_path)


def _generate_and_dump_enriched_ams(id_: str, am: ArreteMinisteriel, parametrization: Parametrization) -> None:
    versions = generate_am_with_versions(am, parametrization, _AM_ID_TO_METADATA[id_])
    if versions.am_versions:
        _dump_am_versions(id_, versions.am_versions)


def _safe_enrich(am: Optional[ArreteMinisteriel], md: AMMetadata) -> ArreteMinisteriel:
    try:
        return enrich(ensure_not_none(am), md)
    except Exception:
        print(md.cid)
        raise


def safe_load_id_to_text() -> Dict[str, ArreteMinisteriel]:
    id_to_text = load_ams(set(list(_AM_ID_TO_METADATA.keys())))
    return {
        id_: _safe_enrich(id_to_text.get(id_), md) for id_, md in tqdm(_AM_ID_TO_METADATA.items(), 'Building AM list.')
    }


def _load_1510_am_no_date() -> List[Dict[str, Any]]:
    return [
        json.load(open(os.path.join(ENRICHED_OUTPUT_FOLDER, f'JORFTEXT000034429274_reg_{regime_str}_no_date.json')))
        for regime_str in ['A', 'E', 'D']
    ]


def _remove_previously_enriched_ams() -> None:
    for file_ in typed_tqdm(os.listdir(ENRICHED_OUTPUT_FOLDER), 'Removing previously enriched files'):
        os.remove(os.path.join(ENRICHED_OUTPUT_FOLDER, file_))


def _generate_enriched_ams(id_to_am: Dict[str, ArreteMinisteriel]) -> None:
    parametrizations = DATA_FETCHER.load_all_parametrizations()
    statuses = DATA_FETCHER.load_all_am_statuses()
    for id_ in tqdm(_AM_ID_TO_METADATA, 'Enriching AM.'):
        parametrization = parametrizations.get(id_) if statuses[id_] == AMStatus.VALIDATED else None
        parametrization = parametrization or Parametrization([], [], [])
        _generate_and_dump_enriched_ams(id_, id_to_am[id_], parametrization)


def _create_if_inexistent(folder: str):
    if not os.path.exists(folder):
        os.mkdir(folder)


def generate_ams() -> None:
    _create_if_inexistent(ENRICHED_OUTPUT_FOLDER)
    id_to_am = safe_load_id_to_text()
    _remove_previously_enriched_ams()
    _generate_enriched_ams(id_to_am)