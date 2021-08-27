'''Evaluate the size distribution of files'''
from typing import List, Tuple

import numpy as np

from tasks.common.ovh import OVHClient


def _fetch_names_and_sizes_of_aps() -> List[Tuple[str, int]]:
    return [(name, size) for name, size in OVHClient.objects_name_and_sizes('ap').items() if name.endswith('.pdf')]


def _size_in_mo(size: int) -> float:
    return size / (1024 ** 2)


def _print_biggest_files(names_and_sizes: List[Tuple[str, int]], nb_files: int) -> None:
    print('Biggest files :')
    for index, (name, size) in enumerate(sorted(names_and_sizes, key=lambda x: x[1], reverse=True)[:nb_files]):
        size_mo = _size_in_mo(size)
        print(f'#{index + 1} {name} {size_mo:.2f} Mo')


def _print_deciles(sizes: List[int]) -> None:
    print('Deciles :')
    deciles = np.percentile(sizes, np.arange(0, 101, 10))
    print(list(map(_size_in_mo, deciles)))


def run():
    names_and_sizes = _fetch_names_and_sizes_of_aps()
    _print_biggest_files(names_and_sizes, 10)
    sizes = [size for _, size in names_and_sizes]
    _print_deciles(sizes)


if __name__ == '__main__':
    run()
