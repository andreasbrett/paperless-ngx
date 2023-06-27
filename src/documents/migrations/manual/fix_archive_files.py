import datetime
import hashlib
import logging
import os
import shutil
from time import sleep

import pathvalidate
from django.conf import settings
from django.template.defaultfilters import slugify

from documents.file_handling import defaultdictNoStr
from documents.file_handling import many_to_dictionary

logger = logging.getLogger("paperless.migrations")


logger = logging.getLogger("paperless.migrations")

###############################################################################
# This is code copied straight paperless before the change.
###############################################################################


def archive_name_from_filename(filename):
    return os.path.splitext(filename)[0] + ".pdf"


def archive_path_old(doc):
    if doc.filename:
        fname = archive_name_from_filename(doc.filename)
    else:
        fname = f"{doc.pk:07}.pdf"

    return os.path.join(settings.ARCHIVE_DIR, fname)


STORAGE_TYPE_GPG = "gpg"


def archive_path_new(doc):
    if doc.archive_filename is not None:
        return os.path.join(settings.ARCHIVE_DIR, str(doc.archive_filename))
    else:
        return None


def source_path(doc):
    if doc.filename:
        fname = str(doc.filename)
    else:
        fname = f"{doc.pk:07}{doc.file_type}"
        if doc.storage_type == STORAGE_TYPE_GPG:
            fname += ".gpg"  # pragma: no cover

    return os.path.join(settings.ORIGINALS_DIR, fname)


def generate_unique_filename(doc, archive_filename=False):
    if archive_filename:
        old_filename = doc.archive_filename
        root = settings.ARCHIVE_DIR
    else:
        old_filename = doc.filename
        root = settings.ORIGINALS_DIR

    counter = 0

    while True:
        new_filename = generate_filename(
            doc,
            counter,
            archive_filename=archive_filename,
        )
        if new_filename == old_filename:
            # still the same as before.
            return new_filename

        if os.path.exists(os.path.join(root, new_filename)):
            counter += 1
        else:
            return new_filename


def generate_filename(doc, counter=0, append_gpg=True, archive_filename=False):
    path = ""

    try:
        if settings.FILENAME_FORMAT is not None:
            tags = defaultdictNoStr(lambda: slugify(None), many_to_dictionary(doc.tags))

            tag_list = pathvalidate.sanitize_filename(
                ",".join(sorted([tag.name for tag in doc.tags.all()])),
                replacement_text="-",
            )

            if doc.correspondent:
                correspondent = pathvalidate.sanitize_filename(
                    doc.correspondent.name,
                    replacement_text="-",
                )
            else:
                correspondent = "none"

            if doc.document_type:
                document_type = pathvalidate.sanitize_filename(
                    doc.document_type.name,
                    replacement_text="-",
                )
            else:
                document_type = "none"

            path = settings.FILENAME_FORMAT.format(
                title=pathvalidate.sanitize_filename(doc.title, replacement_text="-"),
                correspondent=correspondent,
                document_type=document_type,
                created=datetime.date.isoformat(doc.created),
                created_year=doc.created.year if doc.created else "none",
                created_month=f"{doc.created.month:02}" if doc.created else "none",
                created_day=f"{doc.created.day:02}" if doc.created else "none",
                added=datetime.date.isoformat(doc.added),
                added_year=doc.added.year if doc.added else "none",
                added_month=f"{doc.added.month:02}" if doc.added else "none",
                added_day=f"{doc.added.day:02}" if doc.added else "none",
                tags=tags,
                tag_list=tag_list,
            ).strip()

            path = path.strip(os.sep)

    except (ValueError, KeyError, IndexError):
        logger.warning(
            f"Invalid PAPERLESS_FILENAME_FORMAT: "
            f"{settings.FILENAME_FORMAT}, falling back to default",
        )

    counter_str = f"_{counter:02}" if counter else ""

    filetype_str = ".pdf" if archive_filename else doc.file_type

    if len(path) > 0:
        filename = f"{path}{counter_str}{filetype_str}"
    else:
        filename = f"{doc.pk:07}{counter_str}{filetype_str}"

    # Append .gpg for encrypted files
    if append_gpg and doc.storage_type == STORAGE_TYPE_GPG:
        filename += ".gpg"

    return filename


def parse_wrapper(parser, path, mime_type, file_name):
    # this is here so that I can mock this out for testing.
    parser.parse(path, mime_type, file_name)


def create_archive_version(doc, retry_count=3):
    from documents.parsers import DocumentParser
    from documents.parsers import ParseError
    from documents.parsers import get_parser_class_for_mime_type

    logger.info(f"Regenerating archive document for document ID:{doc.id}")
    parser_class = get_parser_class_for_mime_type(doc.mime_type)
    for try_num in range(retry_count):
        parser: DocumentParser = parser_class(None, None)
        try:
            parse_wrapper(
                parser,
                source_path(doc),
                doc.mime_type,
                os.path.basename(doc.filename),
            )
            doc.content = parser.get_text()

            if parser.get_archive_path() and os.path.isfile(parser.get_archive_path()):
                doc.archive_filename = generate_unique_filename(
                    doc,
                    archive_filename=True,
                )
                with open(parser.get_archive_path(), "rb") as f:
                    doc.archive_checksum = hashlib.md5(f.read()).hexdigest()
                os.makedirs(os.path.dirname(archive_path_new(doc)), exist_ok=True)
                shutil.copy2(parser.get_archive_path(), archive_path_new(doc))
            else:
                doc.archive_checksum = None
                logger.error(
                    f"Parser did not return an archive document for document "
                    f"ID:{doc.id}. Removing archive document.",
                )
            doc.save()
            return
        except ParseError:
            if try_num + 1 == retry_count:
                logger.exception(
                    f"Unable to regenerate archive document for ID:{doc.id}. You "
                    f"need to invoke the document_archiver management command "
                    f"manually for that document.",
                )
                doc.archive_checksum = None
                doc.save()
                return
            else:
                # This is mostly here for the tika parser in docker
                # environemnts. The servers for parsing need to come up first,
                # and the docker setup doesn't ensure that tika is running
                # before attempting migrations.
                logger.error("Parse error, will try again in 5 seconds...")
                sleep(5)
        finally:
            parser.cleanup()
