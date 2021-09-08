import json
import os
import re
from datetime import date
from typing import Dict, List, Optional

from envinorma.models import ArreteMinisteriel, StructuredText
from envinorma.utils import typed_tqdm

from ..filenames import ENRICHED_OUTPUT_FOLDER


def _extract_all_references(sections: List[StructuredText]) -> List[Optional[str]]:
    return [section.reference_str for section in sections] + [
        ref for section in sections for ref in _extract_all_references(section.sections)
    ]


def _check_references(am: ArreteMinisteriel) -> None:
    references = _extract_all_references(am.sections)
    nb_refs = len(references)
    if None in references:
        nb_none = len([x for x in references if x is None])
        raise ValueError(f'References cannot be None, found {nb_none}/{nb_refs} None')
    nb_empty_refs = len([x for x in references if not x])
    if nb_empty_refs / (nb_refs or 1) >= 0.95:
        raise ValueError(f'More than 95% of references are empty, found {nb_empty_refs}/{nb_refs} empty')


def _print_input_id(func):
    def _func(am: ArreteMinisteriel):
        try:
            func(am)
        except:  # noqa: E722
            print(am.id)
            raise

    return _func


def _check_date_of_signature(date_of_signature: Optional[date]):
    if not date_of_signature:
        raise ValueError('Expecting date_of_signature to be defined')


def _check_regimes(am: ArreteMinisteriel) -> None:
    # Regime must be A, E or D
    for classement in am.classements:
        assert classement.regime.value in 'AED', f'Invalid regime {classement.regime.value}'


def _check_non_none_fields(am: ArreteMinisteriel) -> None:
    assert am.legifrance_url is not None
    assert am.aida_url is not None


@_print_input_id
def _check_am(am: ArreteMinisteriel) -> None:
    _check_regimes(am)
    _check_references(am)
    _check_non_none_fields(am)
    _check_date_of_signature(am.date_of_signature)


def _load_enriched_am_list(enriched_output_folder: str) -> Dict[str, ArreteMinisteriel]:
    return {
        file_: ArreteMinisteriel.from_dict(json.load(open(os.path.join(enriched_output_folder, file_))))
        for file_ in os.listdir(enriched_output_folder)
    }


_REGEXP = re.compile(r'^(JORFTEXT|LEGITEXT)[0-9]{12}$')


def _check_am_id_format(am_id: str) -> None:
    assert _REGEXP.match(am_id) is not None, f'Invalid AM ID format: {am_id}'


def check_ams() -> None:
    ams = _load_enriched_am_list(ENRICHED_OUTPUT_FOLDER)
    for filename, am in typed_tqdm(ams.items(), 'Checking AMs'):
        _check_am_id_format(filename.split('.json')[0])
        _check_am(am)
    for reg in 'AED':
        assert f'JORFTEXT000034429274_{reg}' not in ams
