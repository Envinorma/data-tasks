'''Build file data/am_id_to_nb_classements.json containing the number of occurrences
of each AM among all active installations.
'''

from collections import Counter
from typing import Dict, List

from tasks.data_build.config import DATA_FETCHER
from tasks.data_build.load import load_classements


def _load_classement_to_am() -> Dict[str, List[str]]:
    classement_to_ams: Dict[str, List[str]] = {}
    for am_id, am in DATA_FETCHER.load_all_am_metadata().items():
        for cl in am.classements:
            cl_str = f'{cl.rubrique}-{cl.regime.value}'
            if cl_str not in classement_to_ams:
                classement_to_ams[cl_str] = []
            classement_to_ams[cl_str].append(am_id)
    return classement_to_ams


def run():
    classement_to_ams = _load_classement_to_am()
    classements = load_classements('all')
    keys = [f'{cl.rubrique}-{cl.regime.to_simple_regime()}' for cl in classements]
    print(Counter([am_id for cl in keys for am_id in classement_to_ams.get(cl) or [None]]))


if __name__ == '__main__':
    run()
