"""Time resolution of the parameters in an AM in order to estimate
the performance if implemented like this in envinorma-web.
"""

from time import time
from typing import Any, Dict, List, Tuple

from envinorma.models import AMMetadata, ArreteMinisteriel, DetailedClassement, DetailedRegime, Parameter, ParameterEnum
from envinorma.parametrization.apply_parameter_values import apply_parameter_values_to_am

from tasks.data_build.config import DATA_FETCHER
from tasks.data_build.load import load_classements
from tasks.data_build.utils import typed_tqdm


def _match(installation_classement: DetailedClassement, metadata: AMMetadata) -> bool:
    for am_classement in metadata.classements:
        if am_classement.rubrique != installation_classement.rubrique:
            continue
        if am_classement.regime.value != installation_classement.regime.value:
            continue
        if not am_classement.alinea:
            return True
        if am_classement.alinea == installation_classement.alinea:
            return True
    return False


def _applicable_am(classement: DetailedClassement, metadata: Dict[str, AMMetadata]) -> List[str]:
    return [am_id for am_id in metadata.keys() if _match(classement, metadata[am_id])]


def _compute_classements_to_am(
    classements: List[DetailedClassement], metadata: Dict[str, AMMetadata]
) -> List[Tuple[DetailedClassement, List[str]]]:
    return [(classement, _applicable_am(classement, metadata)) for classement in classements]


def _group_by_am_id(
    classements_to_am: List[Tuple[DetailedClassement, List[str]]]
) -> Dict[str, List[DetailedClassement]]:
    groups: Dict[str, List[DetailedClassement]] = {}
    for classement, am_ids in classements_to_am:
        for am_id in am_ids:
            if am_id not in groups:
                groups[am_id] = []
            groups[am_id].append(classement)
    return groups


def _date_type(regime: DetailedRegime) -> Parameter:
    if regime == DetailedRegime.A:
        return ParameterEnum.DATE_AUTORISATION.value
    if regime == DetailedRegime.D:
        return ParameterEnum.DATE_DECLARATION.value
    if regime == DetailedRegime.E:
        return ParameterEnum.DATE_ENREGISTREMENT.value
    raise ValueError(f'Unepected regime: {regime}')


def _remove_none_values(parameter_values: Dict[Parameter, Any]) -> Dict[Parameter, Any]:
    return {parameter: value for parameter, value in parameter_values.items() if value is not None}


def _parameter_dict(classements: List[DetailedClassement]) -> Dict[Parameter, Any]:
    if len(classements) == 0:
        raise ValueError('At least one classement is needed')
    if len(classements) > 1:
        return {}  # Parameter values are considered unknown to avoid ambiguity.
    classement = classements[0]
    date = _date_type(classement.regime)
    return _remove_none_values(
        {
            ParameterEnum.REGIME.value: classement.regime.to_regime(),
            ParameterEnum.ALINEA.value: classement.alinea,
            ParameterEnum.RUBRIQUE.value: classement.rubrique,
            ParameterEnum.DATE_INSTALLATION.value: classement.date_mise_en_service,
            date: classement.date_autorisation,
        }
    )


def _fetch_and_apply_parameters(am_id: str, classements: List[DetailedClassement]) -> ArreteMinisteriel:
    am = DATA_FETCHER.safe_load_most_advanced_am(am_id)
    parametrization = DATA_FETCHER.load_or_init_parametrization(am_id)
    parameters = _parameter_dict(classements)
    try:
        return apply_parameter_values_to_am(am, parameters, parametrization)
    except ValueError:
        print(parameters)
        print(am_id)
        raise


def _compute_am_list(classements: List[DetailedClassement]) -> List[ArreteMinisteriel]:
    """Compute the list of AMs from the detailed classements."""
    metadata = DATA_FETCHER.load_all_am_metadata()
    classements_to_am = _compute_classements_to_am(classements, metadata)
    am_to_classements = _group_by_am_id(classements_to_am)
    return [_fetch_and_apply_parameters(am_id, am_classements) for am_id, am_classements in am_to_classements.items()]


def _group_by_installation_id(classements: List[DetailedClassement]) -> Dict[str, List[DetailedClassement]]:
    groups: Dict[str, List[DetailedClassement]] = {}
    for classement in classements:
        if classement.s3ic_id not in groups:
            groups[classement.s3ic_id] = []
        groups[classement.s3ic_id].append(classement)
    return groups


def _time_on_installations_sample() -> None:
    sample_size = 1000
    classement_groups = list(_group_by_installation_id(load_classements('sample')).items())[:sample_size]
    times = []
    nb_errors = 0
    for _, classements in typed_tqdm(classement_groups):
        start = time()
        try:
            _compute_am_list(classements)
        except Exception:
            nb_errors += 1
        times.append(time() - start)
    print(f'{nb_errors}/{sample_size} errors')
    print(f'Mean time: {sum(times) / len(times)}')
    print(f'Max time: {max(times)}')
    print(f'Min time: {min(times)}')
