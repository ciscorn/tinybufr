# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas",
# ]
# ///

import subprocess

import pandas as pd

TABLE_B_HEAD = """
//! This file is generated from BUFRCREX_TableB_en.txt.

use super::{XY, TableBEntry};
"""

TABLE_C_HEAD = """
//! This file is generated from BUFR_TableC_en.txt.

use super::TableCEntry;
"""

TABLE_D_HEAD = """
//! This file is generated from BUFR_TableD_en.txt.

use super::{Descriptor, XY, TableDEntry};
"""


def escape(s: str | float) -> str:
    if isinstance(s, str):
        return s.replace('"', '\\"')
    elif isinstance(s, float):
        return ""
    else:
        raise ValueError(f"Unknown type: {type(s)}")


def make_table_b() -> None:
    df = pd.read_csv("./BUFR4/txt/BUFRCREX_TableB_en.txt")
    with open("./src/tables/table_b.rs", "w") as f:
        f.write(TABLE_B_HEAD)
        f.write("\n")
        f.write(f"pub static TABLE_B: [TableBEntry; {len(df)}] = [\n")
        for _, row in df.iterrows():
            if row["Status"] == "Deprecated":  # type: ignore
                continue
            fxy = row["FXY"]  # type: ignore
            assert fxy < 1000000  # type: ignore
            x = (fxy % 100000) // 1000
            y = fxy % 1000
            f.write("TableBEntry {\n")
            f.write(f"xy: XY {{ x: {x}, y: {y} }},\n")
            f.write(f'class_name: "{row["ClassName_en"]}",\n')
            f.write(f'element_name: "{escape(row["ElementName_en"])}",\n')  # type: ignore
            f.write(f"scale: {row['BUFR_Scale']},\n")
            f.write(f"reference_value: {row['BUFR_ReferenceValue']},\n")
            unit = row["BUFR_Unit"]  # type: ignore
            bits = row["BUFR_DataWidth_Bits"]  # type: ignore
            if bits >= 33:  # type: ignore
                assert unit == "CCITT IA5"  # type: ignore
            f.write(f'unit: "{unit}",\n')
            f.write(f"bits: {bits},\n")
            f.write("},\n")
        f.write("];")


def make_table_c() -> None:
    df = pd.read_csv("./BUFR4/txt/BUFR_TableC_en.txt")
    with open("./src/tables/table_c.rs", "w") as f:
        f.write(TABLE_C_HEAD)
        f.write("\n")
        f.write(f"pub static TABLE_C: [TableCEntry; {len(df)}] = [\n")
        for _, row in df.iterrows():
            if row["Status"] == "Deprecated":  # type: ignore
                continue
            fxy = row["FXY"]  # type: ignore
            assert fxy[0] == "2"  # type: ignore
            x = int(str(fxy)[:3]) % 100  # type: ignore
            if isinstance(fxy, str) and fxy.endswith("YYY"):
                y = None
            else:
                y = int(fxy) % 1000  # type: ignore
            f.write("TableCEntry {\n")
            if y is None:
                f.write(f"xy: ({x}, {None}),\n")
            else:
                f.write(f"xy: ({x}, Some({y})),\n")
            f.write(f'operator_name: "{row["OperatorName_en"]}",\n')
            f.write(f'operation_definition: "{row["OperationDefinition_en"]}",\n')
            f.write("},\n")
        f.write("];")


def make_table_d() -> None:
    df = pd.read_csv("./BUFR4/txt/BUFR_TableD_en.txt")
    count = 0
    previous_fxy = None
    closed = set()
    with open("./src/tables/table_d.rs", "w") as f:
        f.write(TABLE_D_HEAD)
        f.write("\n")

        size = df[df["Status"] != "Deprecated"].FXY1.nunique()  # type: ignore

        f.write(f"pub static TABLE_D: [TableDEntry; {size}] = [\n")
        for _, row in df.iterrows():
            if row["Status"] == "Deprecated":  # type: ignore
                continue
            fxy = row["FXY1"]  # type: ignore
            if fxy == previous_fxy:  # type: ignore
                count += 1
            elif previous_fxy is not None:
                assert previous_fxy not in closed
                closed.add(previous_fxy)
                count = 0
                f.write("],\n")
                f.write("},\n")
            previous_fxy = fxy
            x1 = (fxy % 100000) // 1000
            y1 = fxy % 1000
            fxy2 = row["FXY2"]  # type: ignore
            f2 = fxy2 // 100000
            x2 = (fxy2 % 100000) // 1000
            y2 = fxy2 % 1000
            if count == 0:
                f.write("TableDEntry {\n")
                f.write(f"xy: XY {{ x: {x1}, y: {y1} }},\n")
                f.write(f'category: "{escape(row["CategoryOfSequences_en"])}",\n')  # type: ignore
                f.write(
                    f'title: "{escape(row["Title_en"]).removeprefix("(").removesuffix(")")}",\n'  # type: ignore
                )
                f.write(f'sub_title: "{escape(row["SubTitle_en"])}",\n')  # type: ignore
                f.write("elements: &[\n")
            f.write(f"Descriptor {{ f: {f2}, x: {x2}, y: {y2} }},\n")

        f.write("],\n")
        if count > 0:
            f.write("},\n")
            f.write("];\n")


def main():
    print("generating...")
    make_table_b()
    make_table_c()
    make_table_d()
    print("cargo fmt...")
    subprocess.run(["cargo", "fmt"], check=True)


if __name__ == "__main__":
    main()
