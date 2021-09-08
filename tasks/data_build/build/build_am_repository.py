'''
Generate AM open data repository
cf https://github.com/Envinorma/arretes-ministeriels
'''
import json
import os
from typing import Any, Dict, List, Union

from envinorma.models.arrete_ministeriel import ArreteMinisteriel
from envinorma.parametrization import Parametrization
from envinorma.utils import typed_tqdm

from tasks.data_build.config import AM_REPOSITORY_FOLDER, DATA_FETCHER

_METADATA_FOLDER = os.path.join(AM_REPOSITORY_FOLDER, 'metadata')
_BASE_AMS_FOLDER = os.path.join(AM_REPOSITORY_FOLDER, 'base_ams')
_PARAMETRIZATIONS_FOLDER = os.path.join(AM_REPOSITORY_FOLDER, 'parametrizations')


def _create_if_inexistent(folder: str) -> None:
    if not os.path.exists(folder):
        os.mkdir(folder)


def _dump(object_: Union[Dict, List], filename: str) -> None:
    with open(filename, 'w') as file_:
        json.dump(object_, file_, ensure_ascii=True, indent=2, sort_keys=True)


def _dump_am_metadata(am_id: str, am: Dict[str, Any]) -> None:
    filename = os.path.join(_METADATA_FOLDER, am_id + '.json')
    _dump(am, filename)


def _generate_metadata_folder() -> None:
    _create_if_inexistent(_METADATA_FOLDER)
    metadata = DATA_FETCHER.load_all_am_metadata()
    for am_id, md in typed_tqdm(metadata.items(), 'Dumping AM metadata'):
        _dump_am_metadata(am_id, md.to_dict())


def _dump_am(am: Dict[str, Any]) -> None:
    filename = os.path.join(_BASE_AMS_FOLDER, am['id'] + '.json')
    _dump(am, filename)


def _load_base_ams() -> Dict[str, ArreteMinisteriel]:
    return DATA_FETCHER.load_id_to_most_advanced_am()


def _generate_base_ams_folder() -> None:
    _create_if_inexistent(_BASE_AMS_FOLDER)
    base_ams = _load_base_ams()
    for am in typed_tqdm(base_ams.values(), 'Dumping base AMs'):
        _dump_am(am.to_dict())


def _dump_parametrization(am_id: str, parametrization: Parametrization) -> None:
    filename = os.path.join(_PARAMETRIZATIONS_FOLDER, am_id + '.json')
    _dump(parametrization.to_dict(), filename)


def _generate_parametrizations_folder() -> None:
    _create_if_inexistent(_PARAMETRIZATIONS_FOLDER)
    parametrizations = DATA_FETCHER.load_all_parametrizations()
    for am_id, parametrization in typed_tqdm(parametrizations.items(), 'Dumping parametrizations'):
        _dump_parametrization(am_id, parametrization)


def _empty_directory(folder: str) -> None:
    if os.path.exists(folder):
        for file_ in os.listdir(folder):
            os.remove(os.path.join(folder, file_))


def generate_am_repository() -> None:
    _empty_directory(_METADATA_FOLDER)
    _empty_directory(_BASE_AMS_FOLDER)
    _empty_directory(_PARAMETRIZATIONS_FOLDER)
    _generate_metadata_folder()
    _generate_base_ams_folder()
    _generate_parametrizations_folder()
