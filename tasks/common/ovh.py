import tempfile
from functools import lru_cache
from typing import Any, Callable, Dict, Iterable, List, Literal, TypeVar

from swiftclient.service import SwiftService, SwiftUploadObject

from tasks.common import download_document

BucketName = Literal['ap', 'am', 'misc']
_BASE_BUCKET_URL = 'https://storage.sbg.cloud.ovh.net/v1/AUTH_3287ea227a904f04ad4e8bceb0776108/{}'


def bucket_url(bucket: BucketName) -> str:
    return _BASE_BUCKET_URL.format(bucket)


def _check_upload(results: List[Dict]) -> None:
    for result in results:
        if not result.get('success'):
            raise ValueError(f'Failed Uploading document. Response:\n{result}')


def _check_auth(service: SwiftService) -> None:
    services = list(service.list())
    if len(services) != 1:
        return
    error = services[0].get('error')
    traceback = services[0].get('traceback')
    if error:
        raise ValueError(f'Probable error in authentication: {error}\n{traceback}')
    print('OVH service successfully started.')


@lru_cache
def _get_swift_service() -> SwiftService:
    service = SwiftService()
    _check_auth(service)
    return service


def dump_in_ovh(object_name: str, bucket: BucketName, dumper: Callable[[str], None]) -> None:
    with tempfile.NamedTemporaryFile('w') as file_:
        dumper(file_.name)
        OVHClient.upload_document(bucket, file_.name, object_name)


T = TypeVar('T')


def load_from_ovh(obect_name: str, bucket: BucketName, loader: Callable[[str], T]) -> T:
    with tempfile.NamedTemporaryFile('w') as file_:
        url = bucket_url(bucket) + '/' + obect_name
        download_document(url, file_.name)
        return loader(file_.name)


class OVHClient:
    @staticmethod
    def list_bucket_object_names(bucket: BucketName) -> List[str]:
        lists = list(_get_swift_service().list(bucket))
        return [x['name'] for list_ in lists for x in list_['listing']]

    @staticmethod
    def file_exists(filename: str, bucket_name: BucketName) -> bool:
        results: List[Dict] = list(_get_swift_service().stat(bucket_name, [filename]))  # type: ignore
        return results[0]['success']

    @staticmethod
    def upload_document(bucket_name: BucketName, source: str, destination: str) -> None:
        remote = SwiftUploadObject(source, object_name=destination)
        result = list(_get_swift_service().upload(bucket_name, [remote]))
        _check_upload(result)

    @staticmethod
    def list_bucket_objects(bucket_name: BucketName) -> Iterable[Dict[str, Any]]:
        return _get_swift_service().list(bucket_name)

    @staticmethod
    def objects_name_and_sizes(bucket_name: BucketName) -> Dict[str, int]:
        batches = OVHClient.list_bucket_objects(bucket_name)
        return {element['name']: element['bytes'] for batch in batches for element in batch['listing']}
