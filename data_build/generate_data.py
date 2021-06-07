'''
Download last versions of AM and send them to envinorma-web
'''

from data_build.build.build_ams import generate_ams
from data_build.build.build_aps import dump_ap_datasets, dump_aps
from data_build.build.build_classements import build_all_classement_datasets, build_classements_csv
from data_build.build.build_documents import build_all_document_datasets, download_georisques_documents
from data_build.build.build_georisques_ids import dump_georisques_ids
from data_build.build.build_installations import build_all_installations_datasets, build_installations_csv
from data_build.filenames import AM_LIST_FILENAME, ENRICHED_OUTPUT_FOLDER, dataset_filename
from data_build.validate.check_am import check_ams
from data_build.validate.check_classements import check_classements_csv
from data_build.validate.check_documents import check_documents_csv
from data_build.validate.check_installations import check_installations_csv


def _check_seeds() -> None:
    check_classements_csv(dataset_filename('all', 'classements'))
    check_installations_csv(dataset_filename('all', 'installations'))
    check_documents_csv(dataset_filename('all', 'aps'))
    check_ams(AM_LIST_FILENAME, ENRICHED_OUTPUT_FOLDER)


def run():
    generate_ams()
    # build_installations_csv()
    # build_all_installations_datasets()
    # build_classements_csv()
    # build_all_classement_datasets()
    # download_georisques_documents()
    # build_all_document_datasets()
    # dump_aps('sample')
    # dump_ap_datasets()
    # dump_georisques_ids(filename)
    _check_seeds()


if __name__ == '__main__':
    run()
