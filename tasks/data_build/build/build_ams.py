import os
from typing import Dict, List, Optional

import psycopg2
from envinorma.enriching import enrich
from envinorma.models import AMMetadata, ArreteMinisteriel, Ints
from envinorma.models.condition import OrCondition
from envinorma.models.structured_text import (
    PotentialInapplicability,
    PotentialModification,
    SectionParametrization,
    StructuredText,
)
from envinorma.parametrization import Parametrization
from envinorma.parametrization.models.parametrization import AlternativeSection, AMWarning, InapplicableSection
from envinorma.utils import AMStatus, ensure_not_none, typed_tqdm, write_json
from tqdm import tqdm

from tasks.data_build.config import DATA_FETCHER
from tasks.data_build.filenames import ENRICHED_OUTPUT_FOLDER
from tasks.data_build.load import load_ams

try:
    _AM_ID_TO_METADATA = {
        id_: md for id_, md in DATA_FETCHER.load_all_am_metadata().items() if not id_.startswith('FAKE')
    }
except psycopg2.OperationalError:
    _AM_ID_TO_METADATA = {}


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


def _remove_previously_enriched_ams() -> None:
    for file_ in typed_tqdm(os.listdir(ENRICHED_OUTPUT_FOLDER), 'Removing previously enriched files'):
        os.remove(os.path.join(ENRICHED_OUTPUT_FOLDER, file_))


def _create_if_inexistent(folder: str):
    if not os.path.exists(folder):
        os.mkdir(folder)


def _inapplicabilities(inapplicable_sections: List[InapplicableSection]) -> List[PotentialInapplicability]:
    return [
        PotentialInapplicability(
            inapplicable_section.condition, inapplicable_section.targeted_entity.outer_alinea_indices
        )
        for inapplicable_section in inapplicable_sections
    ]


def _modifications(alternative_sections: List[AlternativeSection]) -> List[PotentialModification]:
    return [
        PotentialModification(alternative_section.condition, alternative_section.new_text)
        for alternative_section in alternative_sections
    ]


def _warnings(warnings: List[AMWarning]) -> List[str]:
    return [warning.text for warning in warnings]


def _init_section_parametrization(parametrization: Parametrization, path: Ints) -> SectionParametrization:
    return SectionParametrization(
        _inapplicabilities(parametrization.path_to_conditions.get(path, [])),
        _modifications(parametrization.path_to_alternative_sections.get(path, [])),
        _warnings(parametrization.path_to_warnings.get(path, [])),
    )


def _add_parametrization_in_section(text: StructuredText, path: Ints, parametrization: Parametrization) -> None:
    text.parametrization = _init_section_parametrization(parametrization, path)
    for i, section in enumerate(text.sections):
        _add_parametrization_in_section(section, path + (i,), parametrization)


def _am_inapplicabilities(am: ArreteMinisteriel, parametrization: Parametrization) -> None:
    am.applicability.warnings = _warnings(parametrization.path_to_warnings.get((), []))
    conditions = [cd.condition for cd in parametrization.path_to_conditions.get((), [])]
    if len(conditions) >= 2:
        am.applicability.condition_of_inapplicability = OrCondition(frozenset(conditions))
    elif len(conditions) == 1:
        am.applicability.condition_of_inapplicability = conditions[0]
    else:
        am.applicability.condition_of_inapplicability = None


def _add_parametrization(am: ArreteMinisteriel, parametrization: Parametrization) -> None:
    _am_inapplicabilities(am, parametrization)
    for i, section in enumerate(am.sections):
        _add_parametrization_in_section(section, (i,), parametrization)


def _add_parametrizations(id_to_am: Dict[str, ArreteMinisteriel]) -> None:
    parametrizations = DATA_FETCHER.load_all_parametrizations()
    statuses = DATA_FETCHER.load_all_am_statuses()
    for id_ in tqdm(_AM_ID_TO_METADATA, 'Adding parametrization in AMs'):
        parametrization = parametrizations.get(id_) if statuses[id_] == AMStatus.VALIDATED else None
        parametrization = parametrization or Parametrization([], [], [])
        _add_parametrization(id_to_am[id_], parametrization)


def _write_ams(id_to_am: Dict[str, ArreteMinisteriel]) -> None:
    for am_id, am in typed_tqdm(id_to_am.items(), 'Writing AMs'):
        full_path = os.path.join(ENRICHED_OUTPUT_FOLDER, am_id) + '.json'
        write_json(am.to_dict(), full_path)


def generate_ams() -> None:
    _create_if_inexistent(ENRICHED_OUTPUT_FOLDER)
    id_to_am = safe_load_id_to_text()
    _add_parametrizations(id_to_am)
    _remove_previously_enriched_ams()
    _write_ams(id_to_am)
