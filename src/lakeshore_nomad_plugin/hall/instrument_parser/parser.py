#
# Copyright The NOMAD Authors.
#
# This file is part of NOMAD. See https://nomad-lab.eu for further info.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from nomad.metainfo import (
    MSection,
    Quantity,
)
from nomad.parsing import MatchingParser
from nomad.datamodel.metainfo.annotations import (
    ELNAnnotation,
)
from nomad.datamodel.data import (
    EntryData,
)

from nomad.datamodel.datamodel import EntryArchive, EntryMetadata

from lakeshore_nomad_plugin.hall.schema import (
    HallInstrument,
)
from lakeshore_nomad_plugin.hall.utils import (
    get_hash_ref,
    create_archive,
    get_instrument,
)
from lakeshore_nomad_plugin.hall import reader as hall_reader


class RawFileLakeshoreInstrument(EntryData):
    instrument = Quantity(
        type=HallInstrument,
        a_eln=ELNAnnotation(
            component="ReferenceEditQuantity",
        ),
    )


class HallInstrumentParser(MatchingParser):
    def parse(self, mainfile: str, archive: EntryArchive, logger) -> None:
        data_file = mainfile.split("/")[-1]
        data_file_with_path = mainfile.split("raw/")[-1]
        filetype = "yaml"
        
        instrument_data = HallInstrument()

        logger.info("Parsing hall measurement instrument file.")
        with archive.m_context.raw_file(
            data_file_with_path, "r", encoding="unicode_escape"
        ) as f:
            data_template = hall_reader.parse_txt(f.name)
            self.instrument = get_instrument(data_template, logger)

        instrument_filename = f"{data_file[:-5]}_instrument.archive.{filetype}"
        instrument_archive = EntryArchive(
            data=instrument_data,
            m_context=archive.m_context,
            metadata=EntryMetadata(upload_id=archive.m_context.upload_id),
        )

        create_archive(
            instrument_archive.m_to_dict(),
            archive.m_context,
            instrument_filename,
            filetype,
            logger,
        )

        archive.data = RawFileLakeshoreInstrument(
            instrument=get_hash_ref(archive.m_context.upload_id, instrument_filename)
        )
        archive.metadata.entry_name = data_file + " instrument file"
