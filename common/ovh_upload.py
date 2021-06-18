from functools import lru_cache
from typing import Dict, List, Literal

from swiftclient.service import SwiftService, SwiftUploadObject

BucketName = Literal['ap', 'am']


def _check_upload(results: List[Dict]) -> None:
    for result in results:
        if not result.get('success'):
            raise ValueError(f'Failed Uploading document. Response:\n{result}')


def upload_document(bucket_name: BucketName, service: SwiftService, source: str, destination: str) -> None:
    remote = SwiftUploadObject(source, object_name=destination)
    result = list(service.upload(bucket_name, [remote]))
    _check_upload(result)


def _check_auth(service: SwiftService) -> None:
    services = list(service.list())
    if len(services) != 1:
        return
    error = services[0].get('error')
    traceback = services[0].get('traceback')
    if error:
        raise ValueError(f'Probable error in authentication: {error}\n{traceback}')
    print('Service successfully started.')


@lru_cache
def init_swift_service() -> SwiftService:
    service = SwiftService()
    _check_auth(service)
    return service
