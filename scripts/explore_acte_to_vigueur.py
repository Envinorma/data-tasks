"""Exploration of the transformation of the classements from classements actes to classements en vigueur
"""

from typing import Callable, Dict, List, Optional, Tuple
from envinorma.models.classement import Classement

from envinorma.models.installation_classement import DetailedClassement, DetailedRegime
from tasks.data_build.load import load_classements
from collections import defaultdict, Counter

_ClassementTuple = Tuple[Optional[str], Optional[DetailedRegime], Optional[str]]
_ClassementCounter = Dict[_ClassementTuple, int]
_ClassementMapping = Dict[_ClassementTuple, _ClassementCounter]
_ClassementOneToOneMapping = Dict[_ClassementTuple, _ClassementTuple]
_ClassementMatcher = Callable[[DetailedClassement], bool]


def _classement_acte(classement: DetailedClassement) -> _ClassementTuple:
    return (classement.rubrique_acte, classement.regime_acte, classement.alinea_acte)


def _classement_vigueur(classement: DetailedClassement) -> _ClassementTuple:
    return (classement.rubrique, classement.regime, classement.alinea)


def _extract_mapping(classements: List[DetailedClassement]) -> _ClassementMapping:
    mapping: _ClassementMapping = defaultdict(Counter)
    for classement in classements:
        tuple_acte = _classement_acte(classement)
        tuple_vigueur = _classement_vigueur(classement)
        mapping[tuple_acte][tuple_vigueur] += 1
    return mapping


def _classement_to_str(classement: _ClassementTuple) -> str:
    return '{} {} {}'.format(*classement)


def _pretty_print_weirdest_mappings(
    mapping: _ClassementMapping, filter_targets: Callable[[_ClassementCounter], bool]
) -> None:
    for classement_acte, targets in sorted(mapping.items(), key=lambda x: sum(x[1].values()), reverse=True):
        if filter_targets(targets):
            print(_classement_to_str(classement_acte))
            for classement_vigueur, nb_occurrences in targets.items():
                print(f'\t{_classement_to_str(classement_vigueur)} ({nb_occurrences})')


def _has_small_divergence(classement_counter: _ClassementCounter) -> bool:
    return len([1 for occs in classement_counter.values() if occs >= 3]) >= 2


def _has_several_frequent_targets(classement_counter: _ClassementCounter) -> bool:
    total_nb_classements = sum(classement_counter.values())
    if total_nb_classements < 10:
        return False
    nb_frequent_targets = len([1 for occs in classement_counter.values() if occs * 10 >= total_nb_classements])
    return nb_frequent_targets >= 2


def _filter_classements(
    classements: List[DetailedClassement], criterion: _ClassementMatcher
) -> List[DetailedClassement]:
    return [classement for classement in classements if criterion(classement)]


def _matcher_classement_acte(classement_acte: _ClassementTuple) -> _ClassementMatcher:
    def _matcher(classement: DetailedClassement) -> bool:
        return _classement_acte(classement) == classement_acte

    return _matcher


def _classement_volume(classement: DetailedClassement) -> float:
    return float(classement.volume.split()[0])


def _pretty_print_classements_with_volume(classements: List[DetailedClassement]) -> None:
    for classement in sorted(classements, key=_classement_volume):
        print(f'{_classement_to_str(_classement_vigueur(classement))} {_classement_volume(classement)}')


def _classement_1530_range(classement: DetailedClassement) -> str:
    if classement.rubrique_acte != '1530':
        raise ValueError('Wrong rubrique.')
    volume = _classement_volume(classement)
    if volume <= 1000:
        return 'volume <= 1000'
    if volume <= 20_000:
        return '1000 < volume <= 20000'
    return 'volume >= 20000'


def _count_1530_confusion_matrix(classements: List[DetailedClassement]) -> None:
    tuples = [(_classement_1530_range(cl), _classement_to_str(_classement_vigueur(cl))) for cl in classements]
    print(Counter(tuples))


def _most_occurring_classement(vigueurs: _ClassementCounter) -> _ClassementTuple:
    return sorted(vigueurs.items(), key=lambda x: x[1])[-1][0]


def _deduce_one_to_one_mapping(mapping: _ClassementMapping) -> _ClassementOneToOneMapping:
    return {acte: _most_occurring_classement(vigueurs) for acte, vigueurs in mapping.items()}


def _compute_mapping_efficiency(mapping: _ClassementMapping, classements: List[DetailedClassement]) -> None:
    one_to_one_mapping = _deduce_one_to_one_mapping(mapping)
    print(
        Counter(
            [
                one_to_one_mapping[_classement_acte(classement)] == _classement_vigueur(classement)
                for classement in classements
            ]
        )
    )


if __name__ == '__main__':
    _CLASSEMENTS = load_classements('all')
    _MAPPING = _extract_mapping(_CLASSEMENTS)
    _pretty_print_weirdest_mappings(_MAPPING, _has_several_frequent_targets)

    _pretty_print_classements_with_volume(
        _filter_classements(_CLASSEMENTS, _matcher_classement_acte(('1530', DetailedRegime.D, '2')))
    )

    _count_1530_confusion_matrix(
        _filter_classements(_CLASSEMENTS, _matcher_classement_acte(('1530', DetailedRegime.D, '2')))
    )

    _compute_mapping_efficiency(_MAPPING, _CLASSEMENTS)
