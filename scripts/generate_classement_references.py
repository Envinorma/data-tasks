'''Generate classement references from georisques CSV for envinorma app.
Deprecated as Georisques files is not updated anymore'''
import os
from collections import Counter
from typing import Set, Tuple, Union

import pandas as pd

from tasks.data_build.config import GEORISQUES_DATA_FOLDER, SEED_FOLDER

_GEORISQUES_ALINEAS_MAPPING = {
    **{f'{first}{second}': f'{first}-{second}' for first in '123' for second in 'abcd'},
    **{f'{first}{second}': f'{first}-{second}' for first in 'AB' for second in 'ab12'},
    **{'B1a': 'B-1-a', 'B2a': 'B-2-a', 'B1b': 'B-1-b', 'B2b': 'B-2-b', '31b': '3-1-b', '31a': '3-1-a', '32': '3-2'},
}


def _replace_alinea(alinea: str) -> str:
    '''Replace alinea in georisques CSV with the corresponding alinea in s3ic nomenclature

    Parameters
    ----------
    alinea : str
        Alinea in georisques CSV, e.g. '31a', not containing necessary '-'

    Returns
    -------
    str
        Alinea in s3ic nomenclature
    '''
    return _GEORISQUES_ALINEAS_MAPPING.get(alinea, alinea)


def generate_unique_classements_from_georisques() -> None:
    '''Generate unique classements from georisques CSV'''
    input_filename = os.path.join(GEORISQUES_DATA_FOLDER, 'IC_ref_nomenclature_ic.csv')
    georisques_classements = pd.read_csv(input_filename, sep=';', header=None)  # type: ignore
    classements_en_vigueur = georisques_classements[georisques_classements[8] == 1]
    final_csv = (
        classements_en_vigueur[[1, 5, 6, 7]]
        .rename(columns={1: 'rubrique', 7: 'regime', 5: 'alinea', 6: 'description'})
        .sort_values(by=['rubrique', 'regime', 'alinea'])
    )
    final_csv.alinea = final_csv.alinea.apply(_replace_alinea)
    final_csv.regime = final_csv.regime.apply(_clean_georisques_regime)
    output_filename = os.path.join(SEED_FOLDER, 'classement_references.csv')
    final_csv.to_csv(output_filename, index=False)


def _clean_georisques_regime(regime: str) -> str:
    return regime if regime != 'DC' else 'D'


def _clean_alinea(alinea: Union[str, float]) -> Union[str, float]:
    if isinstance(alinea, float):
        return alinea
    return alinea.replace('-', '')


_Classement = Tuple[str, str, Union[str, float]]


def _print_not_found_tuples(tuples_counter: Counter[_Classement], unique_classements_tuples: Set[_Classement]) -> None:
    for (rubrique, regime, alinea), count in tuples_counter.most_common():
        if (rubrique, regime, alinea) in unique_classements_tuples:
            continue
        print((rubrique, regime, count))
        print(alinea)
        print([al for rub, reg, al in unique_classements_tuples if rub == rubrique and reg == regime])
        print()
        break


def _print_overlap(tuples_counter: Counter[_Classement], unique_classements_tuples: Set[_Classement]) -> None:
    nb_tuples = sum(tuples_counter.values())
    overlap = 0
    for tuple_, occs in tuples_counter.items():
        if tuple_ in unique_classements_tuples:
            overlap += occs
    print(f'Overlap: {overlap/nb_tuples}')


def _load_unique_classements_tuples() -> Set[_Classement]:
    '''Load unique classements tuples'''
    unique_classements_filename = os.path.join(SEED_FOLDER, 'classement_references.csv')
    unique_classements = pd.read_csv(unique_classements_filename).apply(
        lambda x: (str(x.rubrique), _clean_georisques_regime(x.regime), x.alinea), axis=1
    )
    unique_classements_tuples = set(list(unique_classements))
    return unique_classements_tuples


def _load_classements_counter(clean: bool = True) -> Counter[_Classement]:
    '''Load classements counter'''
    all_classements_filename = os.path.join(SEED_FOLDER, 'classements_all.csv')
    all_classements = pd.read_csv(all_classements_filename)
    all_classements_without_47xx = all_classements[all_classements['rubrique'] != '47xx']
    final_classements_df = all_classements_without_47xx[all_classements_without_47xx.regime.apply(lambda x: x in 'AED')]
    if clean:
        final_classements_df.alinea = final_classements_df.alinea.apply(_clean_alinea)
    tuples = list(final_classements_df.apply(lambda x: (x.rubrique, x.regime, x.alinea), axis=1))
    tuples_counter = Counter(tuples)
    return tuples_counter


def compute_classements_overlap() -> None:
    '''Compute classements overlap'''
    unique_classements_tuples = _load_unique_classements_tuples()
    tuples_counter = _load_classements_counter()
    _print_overlap(tuples_counter, unique_classements_tuples)
    _print_not_found_tuples(tuples_counter, unique_classements_tuples)


if __name__ == '__main__':
    generate_unique_classements_from_georisques()
