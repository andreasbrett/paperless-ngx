import logging
import multiprocessing
import shutil
import tempfile
import time
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Tuple

from django.conf import settings

from documents.parsers import run_convert

logger = logging.getLogger("paperless.migrations")


def _do_convert(work_package: Tuple[Path, Path]):
    existing_thumbnail, converted_thumbnail = work_package
    try:
        logger.info(f"Converting thumbnail: {existing_thumbnail}")

        # Run actual conversion
        run_convert(
            density=300,
            scale="500x5000>",
            alpha="remove",
            strip=True,
            trim=False,
            auto_orient=True,
            input_file=f"{existing_thumbnail}[0]",
            output_file=str(converted_thumbnail),
        )

        # Copy newly created thumbnail to thumbnail directory
        shutil.copy(converted_thumbnail, existing_thumbnail.parent)

        # Remove the PNG version
        existing_thumbnail.unlink()

        logger.info(
            "Conversion to WebP completed, "
            f"replaced {existing_thumbnail.name} with {converted_thumbnail.name}",
        )

    except Exception as e:
        logger.error(f"Error converting thumbnail (existing file unchanged): {e}")


def convert_thumbnails_to_webp(apps, schema_editor):
    start = time.time()

    with tempfile.TemporaryDirectory() as tempdir:
        work_packages = []

        for file in Path(settings.THUMBNAIL_DIR).glob("*.png"):
            existing_thumbnail = file.resolve()

            # Change the existing filename suffix from png to webp
            converted_thumbnail_name = existing_thumbnail.with_suffix(
                ".webp",
            ).name

            # Create the expected output filename in the tempdir
            converted_thumbnail = (
                Path(tempdir) / Path(converted_thumbnail_name)
            ).resolve()

            # Package up the necessary info
            work_packages.append(
                (existing_thumbnail, converted_thumbnail),
            )

        if len(work_packages):
            logger.info(
                "\n\n"
                "  This is a one-time only migration to convert thumbnails for all of your\n"
                "  documents into WebP format.  If you have a lot of documents though, \n"
                "  this may take a while, so a coffee break may be in order."
                "\n",
            )

            with Pool(
                processes=min(multiprocessing.cpu_count(), 4),
                maxtasksperchild=4,
            ) as pool:
                pool.map(_do_convert, work_packages)

                end = time.time()
                duration = end - start

            logger.info(f"Conversion completed in {duration:.3f}s")
