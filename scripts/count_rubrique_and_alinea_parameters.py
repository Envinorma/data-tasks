# Script for counting the occurrences of AM with rubrique and alinea parameters
from typing import Dict, Set

from envinorma.parametrization import Parameter, ParameterEnum, Parametrization

from tasks.data_build.config import DATA_FETCHER


def _am_ids_using_parameter(parametrizations: Dict[str, Parametrization], parameter: Parameter) -> Set[str]:
    result: Set[str] = set()
    for am_id, parametrization in parametrizations.items():
        if parameter in parametrization.extract_parameters():
            result.add(am_id)
    return result


def _load_active_parametrizations() -> Dict[str, Parametrization]:
    parametrizations = DATA_FETCHER.load_all_parametrizations()
    active_am_ids = DATA_FETCHER.load_all_am_metadata().keys()
    return {am_id: parametrization for am_id, parametrization in parametrizations.items() if am_id in active_am_ids}


def run() -> None:
    parametrizations = _load_active_parametrizations()
    am_using_rubrique = _am_ids_using_parameter(parametrizations, ParameterEnum.RUBRIQUE.value)
    print(f'AM using Rubrique: {", ".join(am_using_rubrique)}')
    am_using_alinea = _am_ids_using_parameter(parametrizations, ParameterEnum.ALINEA.value)
    print(f'AM using Alinea: {", ".join(am_using_alinea)}')


if __name__ == '__main__':
    run()
