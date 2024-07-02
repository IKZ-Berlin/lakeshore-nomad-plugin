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
from nomad.datamodel.data import EntryData

from nomad.datamodel.datamodel import EntryArchive, EntryMetadata


from lakeshore_nomad_plugin.hall import reader as hall_reader

from lakeshore_nomad_plugin.hall.schema import (
    ExperimentLakeshoreHall,
    HallMeasurement,
    HallMeasurementReference,
    HallMeasurementResult,
)

from lakeshore_nomad_plugin.hall.utils import (
    get_hash_ref,
    create_archive, 
    get_measurements,)

from lakeshore_nomad_plugin.hall.measurement import (
    GenericMeasurement,
    VariableFieldMeasurement,
)

class RawFileLakeshoreHall(EntryData):
    measurement = Quantity(
        type=HallMeasurement,
        a_eln=ELNAnnotation(
            component="ReferenceEditQuantity",
        ),
    )


class HallMeasurementsParser(MatchingParser):
    def parse(self, mainfile: str, archive: EntryArchive, logger) -> None:
        data_file = mainfile.split("/")[-1]
        data_file_with_path = mainfile.split("raw/")[-1]
        filetype = "yaml"

        hall_data = HallMeasurement(name=f"{data_file[:-4]}_meas")

        logger.info("Parsing hall measurement measurement file.")
        with archive.m_context.raw_file(
            data_file_with_path, "r", encoding="unicode_escape"
        ) as f:
            data_template = hall_reader.parse_txt(f.name)
            hall_data.measurements = list(get_measurements(data_template))

        for measurement in hall_data.measurements:
            if isinstance(measurement, VariableFieldMeasurement):
                if (
                    measurement.measurement_type == "Hall and Resistivity Measurement"
                    and measurement.maximum_field == measurement.minimum_field
                ):
                    logger.info(
                        "This measurement was detected as a single Field Room Temperature one."
                    )
                    hall_data.results.append(
                        HallMeasurementResult(
                            name="Room Temperature measurement",
                            resistivity=measurement.results[0].resistivity,
                            mobility=measurement.results[0].hall_mobility,
                            carrier_concentration=measurement.results[0].carrier_density,
                        )
                    )

        hall_filename = f"{data_file[:-4]}_meas.archive.{filetype}"
        hall_archive = EntryArchive(
            data=hall_data,
            m_context=archive.m_context,
            metadata=EntryMetadata(upload_id=archive.m_context.upload_id),
        )

        create_archive(
            hall_archive.m_to_dict(),
            archive.m_context,
            hall_filename,
            filetype,
            logger,
        )

        archive.data = RawFileLakeshoreHall(
            measurement=get_hash_ref(archive.m_context.upload_id, hall_filename)
        )
        archive.metadata.entry_name = data_file + " measurement file"
        exp_file_name = f"{data_file[:-4]}_exp.archive.{filetype}"
        experiment_archive = EntryArchive(
            data=ExperimentLakeshoreHall(
                measurement=[
                    HallMeasurementReference(
                        name=f"{data_file[:-4]}_meas",
                        reference=get_hash_ref(archive.m_context.upload_id, hall_filename)
                    )
                ]
            ),
            m_context=archive.m_context,
            metadata=EntryMetadata(upload_id=archive.m_context.upload_id),
        )
        create_archive(
            experiment_archive.m_to_dict(),
            archive.m_context,
            exp_file_name,
            filetype,
            logger,
        )
