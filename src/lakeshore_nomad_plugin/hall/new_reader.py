import re
import json
from typing import Dict, List

from nomad.units import ureg

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
                print(f"LINE {line}")
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
    return (
        dictionary[key] * ureg(dictionary[f"{key}_unit"]).to_base_units().magnitude
        if key in dictionary and f"{key}_unit" in dictionary
        else dictionary[key]
    )


def populate_archive():
    data = parse_file("hall_measurement.txt")
    measurement_objects = []
    for meas_step_key, meas_step in data["Measurements"].items():
        if meas_step_key == "Variable Temperature Measurement":
            measurement_objects.append(
                VariableTemperatureMeasurement(
                    start_time=meas_step["Start Time"],
                    time_completed=meas_step["Time Completed"],
                    elapsed_time=meas_step["Elapsed Time"],
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
                    current_reversal=meas_step["Current Reversal"],
                    geometry_selection=meas_step["Geometry Selection"],
                )
            )
        if meas_step_key == "Variable Field Measurement":
            measurement_objects.append(
                VariableFieldMeasurement(
                    start_time=meas_step["Start Time"],
                    time_completed=meas_step["Time Completed"],
                    elapsed_time=meas_step["Elapsed Time"],
                    field_profile=meas_step["Field profile"],
                    maximum_field=fill_quantity(meas_step, "Maximum Field"),
                    minimum_field=fill_quantity(meas_step, "Minimum Field"),
                    field_step=fill_quantity(meas_step, "Field Step"),
                    direction=meas_step["Direction"],
                    measurement_type=meas_step["Measurement Type"],
                    excitation_current=fill_quantity(meas_step, "Excitation Current"),
                    resistance_range=meas_step["Resistance Range"],
                    dwell_time=fill_quantity(meas_step, "Dwell Time"),
                    current_reversal=meas_step["Current Reversal"],
                    geometry_selection=meas_step["Geometry Selection"],
                    use_zero_field_resistivity=meas_step[
                        "Use Zero-field Resistivity to calculate Hall Mobility"
                    ],
                    zero_field_resistivity=fill_quantity(
                        meas_step, "Zero-field Resistivity"
                    ),
                    field_at_zero_resistivity=fill_quantity(meas_step, "at Field"),
                    temperature_at_zero_resistivity=fill_quantity(
                        meas_step, "at Temperature"
                    ),
                )
            )
        if meas_step == "IV Curve Measurement":
            measurement_objects.append(
                IVCurveMeasurement(
                    start_time=meas_step["Start Time"],
                    time_completed=meas_step["Time Completed"],
                    elapsed_time=meas_step["Elapsed Time"],
                    starting_current=fill_quantity(meas_step, "Starting Current"),
                    ending_current=fill_quantity(meas_step, "Ending Current"),
                    current_step=fill_quantity(meas_step, "Current Step"),
                    resistance_range=meas_step["Resistance Range"],
                    dwell_time=fill_quantity(meas_step, "Dwell Time"),
                )
            )
        else:
            measurement_objects.append(GenericMeasurement())
    return measurement_objects
