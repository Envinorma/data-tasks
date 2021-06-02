import json
import math
import os
from datetime import date
from typing import Dict, List, Optional, Set, Tuple

from envinorma.data import ArreteMinisteriel, DateParameterDescriptor, StructuredText, VersionDescriptor
from envinorma.data.text_elements import Table
from envinorma.utils import AM1510_IDS, ensure_not_none, typed_tqdm


def _extract_all_references(sections: List[StructuredText]) -> List[Optional[str]]:
    return [section.reference_str for section in sections] + [
        ref for section in sections for ref in _extract_all_references(section.sections)
    ]


def _check_references(am: ArreteMinisteriel) -> None:
    references = _extract_all_references(am.sections)
    nb_refs = len(references)
    if None in references:
        nb_none = len([x for x in references if x is None])
        raise ValueError(f'References must all be not None, found {nb_none}/{nb_refs} None')
    nb_empty_refs = len([x for x in references if not x])
    if nb_empty_refs / (nb_refs or 1) >= 0.95:
        raise ValueError(f'More than 95% of references are empty, found {nb_empty_refs}/{nb_refs} empty')


def _extract_all_tables(sections: List[StructuredText]) -> List[Table]:
    return [alinea.table for section in sections for alinea in section.outer_alineas if alinea.table] + [
        tb for section in sections for tb in _extract_all_tables(section.sections)
    ]


def _check_table_extraction(am: ArreteMinisteriel) -> None:
    tables = _extract_all_tables(am.sections)
    str_rows = [row.text_in_inspection_sheet for table in tables for row in table.rows if not row.is_header]
    nb_none = len([x for x in str_rows if x is None])
    nb_rows = len(str_rows)
    if nb_none:
        raise ValueError(f'text_in_inspection_sheet must all be not None, found {nb_none}/{nb_rows} None')
    nb_empty_str_rows = len([x for x in str_rows if not x])
    if nb_empty_str_rows / (nb_rows or 1) >= 0.95:
        raise ValueError(
            f'More than 95% of text_in_inspection_sheet are empty, found {nb_empty_str_rows}/{nb_rows} empty'
        )


_Segment = Tuple[float, float]


def _is_a_partition(segments: List[_Segment]) -> bool:
    segments = sorted(segments)
    if not segments:
        return False
    if segments[0][0] != -math.inf:
        return False
    if segments[-1][1] != math.inf:
        return False
    for (_, l), (r, _) in zip(segments, segments[1:]):
        if l != r:
            return False
    return True


_DatePair = Tuple[Optional[date], Optional[date]]


def _extract_ordinal(date_: Optional[date]) -> Optional[int]:
    return date_.toordinal() if date_ else None


def _is_date_partition(criteria: List[_DatePair]) -> bool:
    segments: List[_Segment] = [
        (_extract_ordinal(left_date) or -math.inf, _extract_ordinal(right_date) or math.inf)
        for left_date, right_date in criteria
    ]
    return _is_a_partition(segments)


def _group_installation_date_by_aed_date(
    versions: List[VersionDescriptor],
) -> Dict[DateParameterDescriptor, List[DateParameterDescriptor]]:
    res: Dict[DateParameterDescriptor, List[DateParameterDescriptor]] = {}
    for app in versions:
        if app.aed_date not in res:
            res[app.aed_date] = []
        res[app.aed_date].append(app.installation_date)
    return res


def _assert_is_partition(used_date_parameters: List[DateParameterDescriptor]) -> None:
    if len(used_date_parameters) == 1:
        assert not used_date_parameters[0].is_used
        return
    date_tuples: List[_DatePair] = []
    nb_parameter_is_not_known = 0
    for parameter in used_date_parameters:
        if not parameter.known_value:
            nb_parameter_is_not_known += 1
            continue
        date_tuples.append((parameter.left_value, parameter.right_value))
    if not _is_date_partition(date_tuples):
        raise ValueError(f'Expecting partition, this is not the case here {date_tuples}')
    if nb_parameter_is_not_known != 1:
        raise ValueError(f'Expecting exactly one version with no date criterion, got {nb_parameter_is_not_known}')


def _assert_is_partition_matrix(versions: List[VersionDescriptor]) -> None:
    groups = _group_installation_date_by_aed_date(versions)
    for candidate_partition in groups.values():
        _assert_is_partition(candidate_partition)
    _assert_is_partition(list(groups.keys()))


def _check_non_overlapping_installation_dates(ams: Dict[str, ArreteMinisteriel]) -> None:
    if len(ams) == 1:
        am = list(ams.values())[0]
        app = am.version_descriptor
        if app.aed_date.is_used or app.installation_date.is_used:
            raise ValueError(
                'Expecting aed date and installation date to not be used in this case. '
                f'Got {app.aed_date.is_used} and {app.installation_date.is_used}.'
            )
        return
    _assert_is_partition_matrix([ensure_not_none(am.version_descriptor) for am in ams.values()])


def _is_default(am: ArreteMinisteriel) -> bool:
    version_descriptor = ensure_not_none(am.version_descriptor)
    return (
        version_descriptor.applicable
        and not version_descriptor.aed_date.known_value
        and not version_descriptor.installation_date.known_value
    )


def _check_exactly_one_non_enriched_am(ams: Dict[str, ArreteMinisteriel]) -> None:
    default_am = [am for am in ams.values() if _is_default(am)]
    if len(default_am) != 1:
        raise ValueError(f'Expecting only one default AM, got {len(default_am)}')


def _check_enriched_am_group(ams: Dict[str, ArreteMinisteriel]) -> None:
    ids = {am.id for am in ams.values()}
    if len(ids) != 1:
        raise ValueError(f'Expecting exactly one am_id in list, got ids={ids}')
    _check_non_overlapping_installation_dates(ams)
    _check_exactly_one_non_enriched_am(ams)


def _print_input_id(func):
    def _func(am: ArreteMinisteriel):
        try:
            func(am)
        except:
            print(am.id)
            raise

    return _func


def _check_date_of_signature(date_of_signature: Optional[date]):
    if not date_of_signature:
        raise ValueError('Expecting date_of_signature to be defined')


def _check_regimes(am: ArreteMinisteriel) -> None:
    regimes = {clas.regime for clas in am.classements}
    if len(regimes) != 1:
        raise ValueError(regimes)


def _check_non_none_fields(am: ArreteMinisteriel) -> None:
    assert am.legifrance_url is not None
    assert am.aida_url is not None


@_print_input_id
def _check_am(am: ArreteMinisteriel) -> None:
    if am.id in AM1510_IDS:
        raise ValueError('1510 should not be in AM list as such')
    _check_regimes(am)
    _check_references(am)
    _check_table_extraction(am)
    _check_non_none_fields(am)
    _check_date_of_signature(am.date_of_signature)
    # Below date must be kept as long as publication_date keeps being used in web app, remove after
    # (because publication_date is not the right term.)
    _check_date_of_signature(am.publication_date)


def _load_am_list(am_list_filename: str) -> List[ArreteMinisteriel]:
    return [ArreteMinisteriel.from_dict(x) for x in json.load(open(am_list_filename))]


def _load_enriched_am_list(enriched_output_folder: str) -> Dict[str, ArreteMinisteriel]:
    return {
        file_: ArreteMinisteriel.from_dict(json.load(open(os.path.join(enriched_output_folder, file_))))
        for file_ in os.listdir(enriched_output_folder)
    }


def _group_enriched_ams(enriched_ams: Dict[str, ArreteMinisteriel]) -> Dict[str, Dict[str, ArreteMinisteriel]]:
    id_to_versions: Dict[str, Dict[str, ArreteMinisteriel]] = {}
    for version_name, am in typed_tqdm(enriched_ams.items(), 'Checking enriched AMs'):
        if am.id not in id_to_versions:
            id_to_versions[am.id or ''] = {}
        id_to_versions[am.id or ''][version_name] = am
    return id_to_versions


def _check_enriched_ams(all_ids: Set[str], enriched_output_folder: str) -> None:
    enriched_ams = _load_enriched_am_list(enriched_output_folder)
    id_to_versions = _group_enriched_ams(enriched_ams)
    for am in typed_tqdm(enriched_ams.values(), 'Checking enriched AMs'):
        _check_am(am)
    assert not (all_ids - set(list(id_to_versions.keys())))
    for am_id, am_versions in typed_tqdm(id_to_versions.items(), 'Checking enriched AM groups'):
        try:
            _check_enriched_am_group(am_versions)
        except Exception:
            print(am_id)
            raise


def check_ams(am_list_filename: str, enriched_output_folder: str) -> None:
    am_list = _load_am_list(am_list_filename)
    for am in typed_tqdm(am_list, 'Checking AMs'):
        _check_am(am)
    all_ids = {ensure_not_none(am.id) for am in am_list}
    for reg in 'AED':
        assert f'JORFTEXT000034429274_{reg}' in all_ids
    _check_enriched_ams(all_ids, enriched_output_folder)
