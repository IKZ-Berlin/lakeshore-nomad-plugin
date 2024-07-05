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


from typing import Dict, List
from datetime import datetime, timedelta
import re
import numpy as np

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


from nomad.units import ureg
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
    get_measurements,
)


from lakeshore_nomad_plugin.hall.measurement import (
    GenericMeasurement,
    VariableTemperatureMeasurement,
    VariableTemperatureResults,
    VariableFieldMeasurement,
    VariableFieldResults,
    IVCurveMeasurement,
    IVResults,
)


def parse_file(filepath):
    with open(filepath, "r", encoding="latin1") as file:
        content = file.read()

    sections_pattern = r"\[(.*?)\](.*?)(?=\n\[|\Z)"
    sections = re.findall(sections_pattern, content, re.DOTALL)

    data_dict = {}
    for section_name, section_content in sections:
        data_dict[section_name] = {}

        if section_name == "Measurements":
            steps_pattern = r"<Step\s*\d+:\s*(.*?)>(.*?)(?=\n<Step|\Z)"
            steps = re.findall(steps_pattern, section_content, re.DOTALL)

            for step_name, step_content in steps:
                step_dict = {}

                contact_sets_pattern = (
                    r"(Contact Sets:.*?)(?=\n\nContact Sets:|\n\n<|\Z)"
                )
                contact_sets_content = re.findall(
                    contact_sets_pattern, step_content, re.DOTALL
                )
                params_lists = []
                for contact_set_content in contact_sets_content:
                    lines = contact_set_content.strip().split("\n")
                    contact_set = {"Name": re.split(r"[=|:]", lines[0])[1].strip()}
                    for index, line in enumerate(lines[1:]):
                        parts = re.split(r"[=|:]", line, maxsplit=1)
                        if len(parts) == 2:
                            key, value = parts
                            value = value.strip()
                            if "[" in key and "]" in key:
                                base_key, unit = key.split("[", 1)
                                base_key = base_key.strip()
                                unit = unit.split("]", 1)[0].strip()
                                contact_set[base_key] = value
                                contact_set[f"{base_key}_unit"] = unit
                            elif "[" in value and "]" in value:
                                contact_set[key.strip()] = value.split("[")[0].strip()
                                contact_set[f"{key.strip()}_unit"] = (
                                    value.split("[")[1].split("]")[0].strip()
                                )
                            else:
                                contact_set[key.strip()] = value
                        else:
                            data_rows = [row.split("\t") for row in lines[index + 1 :]]
                            for parameter in data_rows[0]:
                                contact_set[parameter.split("[")[0].strip()] = [
                                    value[0] for value in data_rows[1:] if value
                                ]
                                contact_set[
                                    f"{parameter.split('[')[0].strip()}_unit"
                                ] = (parameter.split("[")[1].split("]")[0].strip())
                            break
                    params_lists.append(contact_set)

                current_chunk = []
                for line in step_content.split("\n"):
                    line = line.strip()
                    if line:
                        current_chunk.append(line)
                    if not line:
                        if current_chunk:
                            if "Contact Sets" in current_chunk[0]:
                                step_dict["Contact Sets"] = params_lists
                                break
                            for index, line in enumerate(current_chunk):
                                parts = re.split(r"[=|:]", line, maxsplit=1)
                                if len(parts) == 2:
                                    key, value = parts
                                    value = value.strip()
                                    if "[" in key and "]" in key:
                                        base_key, unit = key.split("[", 1)
                                        base_key = base_key.strip()
                                        unit = unit.split("]", 1)[0].strip()
                                        step_dict[base_key] = value
                                        step_dict[f"{base_key}_unit"] = unit
                                    elif "[" in value and "]" in value:
                                        step_dict[key.strip()] = value.split("[")[
                                            0
                                        ].strip()
                                        step_dict[f"{key.strip()}_unit"] = (
                                            value.split("[")[1].split("]")[0].strip()
                                        )
                                    else:
                                        step_dict[key.strip()] = value
                                else:
                                    data_rows = [
                                        row.split("\t") for row in current_chunk
                                    ]
                                    for parameter in data_rows[0]:
                                        step_dict[parameter.split("[")[0].strip()] = [
                                            value[0] for value in data_rows[1:] if value
                                        ]
                                        step_dict[
                                            f"{parameter.split('[')[0].strip()}_unit"
                                        ] = (
                                            parameter.split("[")[1]
                                            .split("]")[0]
                                            .strip()
                                        )
                                    break
                            current_chunk = []

                data_dict[section_name][f"{step_name}"] = step_dict
        else:
            step_dict = {}
            for index, line in enumerate(
                line for line in section_content.split("\n") if line.strip()
            ):
                parts = re.split(r"[=|:]", line, maxsplit=1)
                if len(parts) == 2:
                    key, value = parts
                    value = value.strip()
                    if "[" in key and "]" in key:
                        base_key, unit = key.split("[", 1)
                        base_key = base_key.strip()
                        unit = unit.split("]", 1)[0].strip()
                        step_dict[base_key] = value
                        step_dict[f"{base_key}_unit"] = unit
                    elif "[" in value and "]" in value:
                        step_dict[key.strip()] = value.split("[")[0].strip()
                        step_dict[f"{key.strip()}_unit"] = (
                            value.split("[")[1].split("]")[0].strip()
                        )
                    else:
                        step_dict[key.strip()] = value
                else:
                    step_dict[line] = line

            data_dict[section_name] = step_dict

    return data_dict


def fill_quantity(dictionary: Dict, key: str):
    if f"{key}_unit" in dictionary:
        if "Sec" in dictionary[f"{key}_unit"]:
            dictionary[f"{key}_unit"] = dictionary[f"{key}_unit"].replace("Sec", "s")
        if "\u00b5A" in dictionary[f"{key}_unit"]:
            dictionary[f"{key}_unit"] = "uA"
        if "cm\u00b2" in dictionary[f"{key}_unit"]:
            dictionary[f"{key}_unit"] = dictionary[f"{key}_unit"].replace(
                "\u00b2", "cm^2"
            )
        if "cm\u00b3" in dictionary[f"{key}_unit"]:
            dictionary[f"{key}_unit"] = dictionary[f"{key}_unit"].replace(
                "\u00b2", "cm^3"
            )
        if "VS" in dictionary[f"{key}_unit"]:
            dictionary[f"{key}_unit"] = dictionary[f"{key}_unit"].replace(
                "VS", "volt * second"
            )
        if "ohm cm" in dictionary[f"{key}_unit"]:
            dictionary[f"{key}_unit"] = dictionary[f"{key}_unit"].replace(
                "ohm cm", "ohm * cm"
            )
    if dictionary[key] == "ERROR":
        return None
    if dictionary[key] == "On" or dictionary[key] == "Yes":
        return True
    if isinstance(dictionary[key], list):
        return (
            np.array(dictionary[key], dtype=float)
            * ureg(dictionary[f"{key}_unit"]).to_base_units().magnitude
            if key in dictionary and f"{key}_unit" in dictionary
            else np.array(dictionary[key], dtype=float)
        )
    return (
        float(dictionary[key])
        * ureg(dictionary[f"{key}_unit"]).to_base_units().magnitude
        if key in dictionary and f"{key}_unit" in dictionary
        else float(dictionary[key])
    )


def calc_best_fit_values(contact_set: Dict):
    if (
        contact_set["Current"]
        and contact_set["Best Fit Resistance"]
        and contact_set["Best Fit Offset"]
    ):
        return np.array(contact_set["Current"], dtype=float) * float(
            contact_set["Best Fit Resistance"]
        ) + float(contact_set["Best Fit Offset"])
    return None


def populate_archive(data: Dict):
    # data = parse_file(
    #     "/home/andrea/NOMAD/PLUGINS/lakeshore-nomad-plugin/tests/data/hall/21-032-G_Hall-RT.txt"
    # )
    measurement_objects = []
    for meas_step_key, meas_step in data["Measurements"].items():
        if meas_step_key == "Variable Temperature Measurement":
            measurement_objects.append(
                VariableTemperatureMeasurement(
                    start_time=meas_step["Start Time"],
                    time_completed=meas_step["Time Completed"],
                    elapsed_time=(
                        datetime.strptime(meas_step["Elapsed Time"], "%H:%M:%S")
                        - datetime.strptime("0:00:00", "%H:%M:%S")
                    ).total_seconds(),
                    starting_temperature=fill_quantity(
                        meas_step, "Starting Temperature"
                    ),
                    ending_temperature=fill_quantity(meas_step, "Ending Temperature"),
                    spacing=meas_step["Spacing"],
                    temperature_step=fill_quantity(meas_step, "Temperature Step"),
                    field_at=fill_quantity(meas_step, "Field at"),
                    measurement_type=fill_quantity(meas_step, "Measurement Type"),
                    excitation_current=fill_quantity(meas_step, "Excitation Current"),
                    resistance_range=meas_step["Resistance Range"],
                    dwell_time=fill_quantity(meas_step, "Dwell Time"),
                    current_reversal=fill_quantity(meas_step, "Current Reversal"),
                    geometry_selection=meas_step["Geometry selection"],
                )
            )
        if meas_step_key == "Variable Field Measurement":
            measurement_objects.append(
                VariableFieldMeasurement(
                    start_time=meas_step["Start Time"],
                    time_completed=meas_step["Time Completed"],
                    elapsed_time=(
                        datetime.strptime(meas_step["Elapsed Time"], "%H:%M:%S")
                        - datetime.strptime("0:00:00", "%H:%M:%S")
                    ).total_seconds(),
                    field_profile=meas_step["Field profile"],
                    maximum_field=fill_quantity(meas_step, "Maximum Field"),
                    minimum_field=fill_quantity(meas_step, "Minimum Field"),
                    field_step=fill_quantity(meas_step, "Field Step"),
                    direction=meas_step["Direction"],
                    measurement_type=meas_step["Measurement Type"],
                    excitation_current=fill_quantity(meas_step, "Excitation Current"),
                    resistance_range=meas_step["Resistance Range"],
                    dwell_time=fill_quantity(meas_step, "Dwell Time"),
                    current_reversal=fill_quantity(meas_step, "Current Reversal"),
                    geometry_selection=meas_step["Geometry selection"],
                    use_zero_field_resistivity=fill_quantity(
                        meas_step,
                        "Use Zero-field Resistivity to calculate Hall Mobility",
                    ),
                    zero_field_resistivity=fill_quantity(
                        meas_step, "Zero-field Resistivity"
                    ),
                    field_at_zero_resistivity=fill_quantity(meas_step, "at Field"),
                    temperature_at_zero_resistivity=fill_quantity(
                        meas_step, "at Temperature"
                    ),
                    results=[
                        VariableFieldResults(
                            field=fill_quantity(meas_step, "Field"),
                            resistivity=fill_quantity(meas_step, "Resistivity"),
                            hall_coefficient=fill_quantity(
                                meas_step, "Hall Coefficient"
                            ),
                            carrier_density=fill_quantity(meas_step, "Carrier Density"),
                            hall_mobility=fill_quantity(meas_step, "Hall Mobility"),
                            temperature=fill_quantity(meas_step, "Temperature"),
                        )
                    ],
                )
            )
        if meas_step_key == "IV Curve Measurement":
            contact_sets_objects = []
            for contact_set in meas_step["Contact Sets"]:
                contact_sets_objects.append(
                    IVResults(
                        name=contact_set["Name"],
                        best_fit_resistance=fill_quantity(
                            contact_set, "Best Fit Resistance"
                        ),
                        best_fit_offset=fill_quantity(contact_set, "Best Fit Offset"),
                        correlation=fill_quantity(contact_set, "Correlation"),
                        best_fit_values=calc_best_fit_values(contact_set),
                        current=fill_quantity(contact_set, "Current"),
                        voltage=fill_quantity(contact_set, "Voltage"),
                        field=fill_quantity(contact_set, "Field"),
                        temperature=fill_quantity(contact_set, "Temperature"),
                    )
                )
            measurement_objects.append(
                IVCurveMeasurement(
                    start_time=meas_step["Start Time"],
                    time_completed=meas_step["Time Completed"],
                    elapsed_time=(
                        datetime.strptime(meas_step["Elapsed Time"], "%H:%M:%S")
                        - datetime.strptime("0:00:00", "%H:%M:%S")
                    ).total_seconds(),
                    starting_current=fill_quantity(meas_step, "Starting Current"),
                    ending_current=fill_quantity(meas_step, "Ending Current"),
                    current_step=fill_quantity(meas_step, "Current Step"),
                    resistance_range=meas_step["Resistance Range"],
                    dwell_time=fill_quantity(meas_step, "Dwell Time"),
                    results=contact_sets_objects,
                )
            )
        else:
            measurement_objects.append(GenericMeasurement())
    return measurement_objects


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
            # data_template = hall_reader.parse_txt(f.name)
            data_dict = parse_file(f.name)
            hall_data.measurements = populate_archive(
                data_dict
            )  # list(get_measurements(data_template))

        if (
            len(
                [
                    meas
                    for meas in hall_data.measurements
                    if isinstance(meas, VariableFieldMeasurement)
                ]
            )
            == 1
            and len(
                [
                    meas
                    for meas in hall_data.measurements
                    if isinstance(meas, IVCurveMeasurement)
                ]
            )
            == 1
        ):
            logger.info(
                "This measurement was detected as a Room Temperature single magnetic field."
            )
            measurement = [
                meas
                for meas in hall_data.measurements
                if isinstance(meas, VariableFieldMeasurement)
            ][0]
            hall_data.results.append(
                HallMeasurementResult(
                    name="Room Temperature measurement",
                    resistivity=measurement.results[0].resistivity,
                    mobility=measurement.results[0].hall_mobility,
                    carrier_concentration=measurement.results[0].carrier_density,
                )
            )
            hall_data.tags = ["Room Temperature"]
        if (
            len(
                [
                    meas
                    for meas in hall_data.measurements
                    if isinstance(meas, VariableFieldMeasurement)
                ]
            )
            > 1
        ):
            logger.info("This measurement was detected as a Variable Field.")
            hall_data.tags = ["Variable Field"]
        if (
            len(
                [
                    meas
                    for meas in hall_data.measurements
                    if isinstance(meas, VariableTemperatureMeasurement)
                ]
            )
            >= 1
        ):
            logger.info("This measurement was detected as a Variable Temperature.")
            hall_data.tags = ["Variable Temperature"]

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
                        reference=get_hash_ref(
                            archive.m_context.upload_id, hall_filename
                        ),
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
