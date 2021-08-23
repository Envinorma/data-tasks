'''Evaluate the size distribution of files'''
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
from swiftclient.service import SwiftService

from tasks.common.ovh_upload import BucketName, init_swift_service


def _fetch_bucket_objects(bucket_name: BucketName, service: SwiftService) -> Iterable[Dict[str, Any]]:
    return service.list(bucket_name)


def _fetch_names_and_sizes_of_aps() -> List[Tuple[str, int]]:
    service = init_swift_service()
    batches = _fetch_bucket_objects('ap', service)
    names_and_sizes: List[Tuple[str, int]] = [
        (element['name'], element['bytes'])
        for batch in batches
        for element in batch['listing']
        if element['name'].endswith('.pdf')
    ]
    return names_and_sizes


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
