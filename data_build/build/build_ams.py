import json
import os
from typing import Any, Dict, List, Optional, Set

from envinorma.models import AMMetadata, ArreteMinisteriel
from envinorma.parametrization import Parametrization
from envinorma.parametrization.am_with_versions import AMVersions, apply_parametrization, enrich_am
from envinorma.utils import AM1510_IDS, AMStatus, ensure_not_none, typed_tqdm, write_json
from tqdm import tqdm

from data_build.config import DATA_FETCHER, generate_parametric_descriptor
from data_build.filenames import AM_LIST_FILENAME, ENRICHED_OUTPUT_FOLDER

_AM_ID_TO_METADATA = {id_: md for id_, md in DATA_FETCHER.load_all_am_metadata().items() if not id_.startswith('FAKE')}


def _dump_am_versions(am_id: str, versions: AMVersions) -> None:
    for version_desc, version in versions.items():
        filename = am_id + '_' + generate_parametric_descriptor(version_desc) + '.json'
        full_path = os.path.join(ENRICHED_OUTPUT_FOLDER, filename)
        write_json(version.to_dict(), full_path)


def _generate_and_dump_enriched_ams(id_: str, am: ArreteMinisteriel, parametrization: Parametrization) -> None:
    versions = apply_parametrization(id_, am, parametrization, _AM_ID_TO_METADATA[id_])
    if versions:
        _dump_am_versions(id_, versions)


def _load_id_to_text(ids: Set[str]) -> Dict[str, ArreteMinisteriel]:
    print('loading texts.')
    structured_texts = DATA_FETCHER.load_structured_ams(ids)
    id_to_structured_text = {text.id or '': text for text in structured_texts}
    initial_texts = DATA_FETCHER.load_initial_ams(ids)
    id_to_initial_text = {text.id or '': text for text in initial_texts}
    return {id_: ensure_not_none(id_to_structured_text.get(id_) or id_to_initial_text.get(id_)) for id_ in ids}


def _safe_enrich(am: Optional[ArreteMinisteriel], md: AMMetadata) -> ArreteMinisteriel:
    try:
        return ensure_not_none(enrich_am(ensure_not_none(am), md))
    except Exception:
        print(md.cid)
        raise


def _safe_load_id_to_text() -> Dict[str, ArreteMinisteriel]:
    id_to_text = _load_id_to_text(set(list(_AM_ID_TO_METADATA.keys())))
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


def generate_ams() -> None:
    id_to_am = _safe_load_id_to_text()
    all_ams = [am.to_dict() for am_id, am in id_to_am.items() if am_id not in AM1510_IDS]
    _remove_previously_enriched_ams()
    _generate_enriched_ams(id_to_am)
    all_ams.extend(_load_1510_am_no_date())
    write_json(all_ams, AM_LIST_FILENAME, pretty=False)
