#############################################################################
# zlib License
#
# (C) 2023 Cristóvão Beirão da Cruz e Silva <cbeiraod@cern.ch>
#
# This software is provided 'as-is', without any express or implied
# warranty.  In no event will the authors be held liable for any damages
# arising from the use of this software.
#
# Permission is granted to anyone to use this software for any purpose,
# including commercial applications, and to alter it and redistribute it
# freely, subject to the following restrictions:
#
# 1. The origin of this software must not be misrepresented; you must not
#    claim that you wrote the original software. If you use this software
#    in a product, an acknowledgment in the product documentation would be
#    appreciated but is not required.
# 2. Altered source versions must be plainly marked as such, and must not be
#    misrepresented as being the original software.
# 3. This notice may not be removed or altered from any source distribution.
#############################################################################

from pathlib import Path
import logging
import enum
import datetime
import sqlite3
import hashlib

import pandas

import plotly.express as px
import plotly.graph_objects as go

class CVIV_Types(enum.Enum):
    IV = 0
    IV_Two_Probes = 1
    CV = 2
    # Add new run types here

    def get_type(string: str):
        # Missing the normal IV type of run
        if string == 'IV with two I-meter\n':
            return CVIV_Types.IV_Two_Probes
        elif string == 'CV measurement\n':
            return CVIV_Types.CV
        # Add new run types here

        return None

def is_cviv_run(file_path: Path):
    if file_path.suffix == ".iv" or file_path.suffix == ".cv":
        with open(file_path, "r") as file:
            info_line = file.readline()

            run_type = CVIV_Types.get_type(info_line)
            if run_type is None:
                return False
            return True
    return False

def get_cviv_metadata(file_path: Path, logger: logging.Logger):
    metadata = {
        'path': str(file_path),
        'name': file_path.name,
        'type': None,
    }

    with open(file_path, "r") as file:
        info_line = file.readline()

        metadata['type'] = CVIV_Types.get_type(info_line)

        lines = file.readlines()
        line_idx = 0
        set_voltage = None
        while line_idx < len(lines):
            if lines[line_idx] == ':Program Version\n':
                line_idx += 1
                metadata['version'] = lines[line_idx][:-1]
            elif lines[line_idx] == ':start\n':
                line_idx += 1
                metadata['start'] = datetime.datetime.strptime(lines[line_idx][:-1], "%d/%m/%Y %H:%M:%S")
            elif lines[line_idx] == ':stop\n':
                line_idx += 1
                metadata['stop'] = datetime.datetime.strptime(lines[line_idx][:-1], "%d/%m/%Y %H:%M:%S")
            elif lines[line_idx] == ':elapsed[s]\n':
                line_idx += 1
                metadata['elapsed [s]'] = float(lines[line_idx][:-1])
            elif lines[line_idx] == ':tester\n':
                line_idx += 1
                metadata['tester'] = lines[line_idx][:-1]
            elif lines[line_idx] == ':temperature[C]\n':
                line_idx += 1
                metadata['temperature [C]'] = float(lines[line_idx][:-1])
            elif lines[line_idx] == ':Instruments\n':
                line_idx += 1
                while lines[line_idx][0] != ':':
                    if lines[line_idx][:9] == 'I meter: ':
                        metadata['I meter'] = lines[line_idx][9:-1]
                        metadata['I averaging'] = 5  # For some runs, this number was different (fix below)
                    elif lines[line_idx][:10] == 'V source: ':
                        metadata['V source'] = lines[line_idx][10:-1]
                        if metadata['V source'] == 'Keithley 2410':
                            metadata['V integration time [ms]'] = float(lines[line_idx + 1][:-1].split(': ')[1])
                            metadata['V averaging'] = int(lines[line_idx + 2][:-1].split(': ')[1])
                            metadata['compliance [A]'] = float(lines[line_idx + 3][:-1].split(': ')[1])
                            line_idx += 3
                        else:
                            raise RuntimeError(f'Unknown V Source instrument type ({metadata["V source"]}), in run {str(file_path)}')
                    elif lines[line_idx][:11] == 'LCR meter: ':
                        metadata['LCR meter'] = lines[line_idx][11:-1]
                        if metadata['LCR meter'] == 'Agilent E4980A':
                            metadata['LCR frequency [Hz]'] = int(lines[line_idx + 1][:-1].split(' ')[2])
                            metadata['LCR signal level [V]'] = float(lines[line_idx + 2][:-1].split(' ')[3])/1000.
                            metadata['LCR averaging'] = int(lines[line_idx + 3][:-1].split(': ')[1])
                            line_idx += 3
                        else:
                            raise RuntimeError(f'Unknown LCR Meter instrument type ({metadata["LCR meter"]}), in run {str(file_path)}')
                    else:
                        logger.error(f'Unknown line in Instruments block: {lines[line_idx][:-1]}')
                    line_idx += 1
                line_idx -= 1
            elif lines[line_idx] == ':LCR open correction: C[F] , G[S]\n':
                line_idx += 1
                params = lines[line_idx][:-1].split(', ')
                metadata['LCR open correction C [F]'] = float(params[0])
                metadata['LCR open correction G [S]'] = float(params[1])
            elif lines[line_idx] == ':ramp up step [V], delay [s], down step [V], delay[s]\n':
                line_idx += 1
                params = lines[line_idx][:-1].split(', ')
                metadata['ramp up step [V]']    = float(params[0])
                metadata['ramp up delay [s]']   = float(params[1])
                metadata['ramp down step [V]']  = float(params[2])
                metadata['ramp down delay [s]'] = float(params[3])
            elif lines[line_idx] == ':step mode\n':
                line_idx += 1
                metadata['V step mode'] = lines[line_idx][:-1]
            elif lines[line_idx] == ':set voltage start [V], voltage stop [V], number of steps\n':
                line_idx += 1
                set_voltage = lines[line_idx][:-1].split(', ')
            elif lines[line_idx] == ':Sample\n':
                line_idx += 1
                metadata['sample'] = lines[line_idx][:-1]
            elif lines[line_idx] == ':Sample_comment\n':
                while lines[line_idx + 1][0] != ':':
                    line_idx += 1
                    if lines[line_idx][:6] == 'pixel ':
                        metadata['pixel'] = lines[line_idx][6:-1]
                        info = lines[line_idx][:-1].split(' ')
                        metadata['pixel row'] = info[1]
                        metadata['pixel col'] = info[2]
                    elif lines[line_idx][:-1] == 'PPS pre-irrad' or lines[line_idx][:-1] == 'FBK sample - pre-irrad':
                        metadata['irradiation flux [p/cm^2]'] = 0.0
                        metadata['irradiated'] = False
                    elif lines[line_idx][:-1] == 'IRRAD 1E16':
                        metadata['irradiation flux [p/cm^2]'] = 1E16
                        metadata['irradiated'] = True
                    elif lines[line_idx][:-1] == 'IRRAD 5E15':
                        metadata['irradiation flux [p/cm^2]'] = 5E15
                        metadata['irradiated'] = True
                    else:
                        if 'comments' not in metadata:
                            metadata['comments'] = ''
                        metadata['comments'] += lines[line_idx]
            # TODO:
            #:Irradiation(Location,Fluence/Dose,Units,Particle,Date)
            #:Annealing_history(time[min],Temp[C],Date)
            elif lines[line_idx] == 'BEGIN\n':
                metadata['begin location'] = line_idx + 1 #  Offset because of the extra readline at the start to get the run type
            elif lines[line_idx] == 'END\n':
                metadata['end location'] = line_idx + 1 #  Offset because of the extra readline at the start to get the run type

            line_idx += 1

        # Fix I averaging for the initial runs
        if 'I averaging' in metadata and metadata['start'] < datetime.datetime(2023, 7, 5, 3, 0, 0):
            metadata['I averaging'] = 1

        # Fetch the voltage steps if needed:
        if metadata['V step mode'] == 'linear':
            metadata['V start [V]'] = float(set_voltage[0])
            metadata['V stop [V]']  = float(set_voltage[1])
            metadata['V steps']     = int(set_voltage[2])
        elif metadata['V step mode'] == 'linear hysteresis':
            metadata['V start [V]'] = float(set_voltage[0])
            metadata['V stop [V]']  = float(set_voltage[1])
            metadata['V steps']     = int(set_voltage[2])
        elif metadata['V step mode'] == 'from file':
            # No action needed for "from file"
            pass
        else:
            raise RuntimeError(f'Unknown voltage step mode ({metadata["V step mode"]}), in run {str(file_path)}')

        if 'pixel' not in metadata:
            raise RuntimeError(f'The pixel which was measured was not defined for run {str(file_path)}')

    # TODO: MD5, SHA
    sha = hashlib.sha256()
    md5 = hashlib.md5()
    with open(file_path, "rb") as file:
        while chunk := file.read(65536):
            sha.update(chunk)
            md5.update(chunk)
    metadata['SHA256'] = sha.hexdigest()
    metadata['MD5'] = md5.hexdigest()

    return metadata

def get_column_info_for_db(colName: str):
    if colName == "path":
        return "TEXT NOT NULL UNIQUE"
    elif colName == "name":
        return "TEXT NOT NULL"
    elif colName == "type":
        return "INTEGER NOT NULL"
    elif colName == "version":
        return "TEXT"
    elif colName == "start":
        return "TEXT NOT NULL"
    elif colName == "stop":
        return "TEXT NOT NULL"
    elif colName == "elapsed [s]":
        return "INTEGER NOT NULL"
    elif colName == "tester":
        return "TEXT NOT NULL"
    elif colName == "temperature [C]":
        return "REAL NOT NULL"
    elif colName == "I meter":
        return "TEXT"
    elif colName == "I averaging":
        return "INTEGER"
    elif colName == "V source":
        return "TEXT NOT NULL"
    elif colName == "V integration time [ms]":
        return "REAL NOT NULL"
    elif colName == "V averaging":
        return "INTEGER NOT NULL"
    elif colName == "compliance [A]":
        return "REAL NOT NULL"
    elif colName == "ramp up step [V]":
        return "REAL NOT NULL"
    elif colName == "ramp up delay [s]":
        return "REAL NOT NULL"
    elif colName == "ramp down step [V]":
        return "REAL NOT NULL"
    elif colName == "ramp down delay [s]":
        return "REAL NOT NULL"
    elif colName == "V step mode":
        return "TEXT NOT NULL"
    elif colName == "sample":
        return "TEXT NOT NULL"
    elif colName == "irradiation flux [p/cm^2]":
        return "REAL NOT NULL"
    elif colName == "irradiated":
        return "INTEGER NOT NULL"
    elif colName == "pixel":
        return "TEXT NOT NULL"
    elif colName == "pixel row":
        return "INTEGER NOT NULL"
    elif colName == "pixel col":
        return "INTEGER NOT NULL"
    elif colName == "begin location":
        return "INTEGER NOT NULL"
    elif colName == "end location":
        return "INTEGER NOT NULL"
    elif colName == "V start [V]":
        return "REAL"
    elif colName == "V stop [V]":
        return "REAL"
    elif colName == "V steps":
        return "INTEGER"
    elif colName == "comments":
        return "TEXT"
    elif colName == "LCR meter":
        return "TEXT"
    elif colName == "LCR frequency [Hz]":
        return "INTEGER"
    elif colName == "LCR signal level [V]":
        return "REAL"
    elif colName == "LCR averaging":
        return "INTEGER"
    elif colName == "LCR open correction C [F]":
        return "REAL"
    elif colName == "LCR open correction G [S]":
        return "REAL"
    elif colName == "SHA256":
        return "TEXT NOT NULL UNIQUE"
    elif colName == "MD5":
        return "TEXT NOT NULL UNIQUE"
    return None

def find_and_sort_cviv_runs(base_path: Path, logger: logging.Logger):
    run_list = []
    for subdir in base_path.iterdir():
        if not subdir.is_dir():
            continue
        for file in subdir.iterdir():
            if not file.is_file():
                continue
            is_cviv = is_cviv_run(file)
            if not is_cviv:
                continue

            metadata = get_cviv_metadata(file, logger)
            run_list += [metadata]

    return sorted(run_list, key=lambda d: d['start'])

def enable_foreign_keys(conn: sqlite3.Connection):
    res = conn.execute("PRAGMA foreign_keys;")
    if res.fetchall()[0][0] == 0:
        conn.execute("PRAGMA foreign_keys = ON;")

def create_run_info_table(conn: sqlite3.Connection, tableName: str, columns: list[str], logger: logging.Logger):
    # Check if table exists
    res = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}';")
    if len(res.fetchall()) == 0:  # If table does not exist
        create_table_sql = f"CREATE TABLE `{tableName}` (`RunID` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `RunName` TEXT NOT NULL UNIQUE"
        for col in columns:
            colInfo = get_column_info_for_db(col)
            if colInfo is None:
                logger.warning(f'Unknown column "{col}", skipping it')
                continue
            create_table_sql += f", `{col}` {colInfo}"
        create_table_sql += ", `Observations` TEXT);"

        conn.execute(create_table_sql)
        #res = conn.execute(create_table_sql)
        #print(res.fetchall())
    else:  # If table exists, add missing columns... TODO: this currently does not work very well...
        for col in columns:
            colInfo = get_column_info_for_db(col)
            if colInfo is None:
                logger.warning(f'Unknown column "{col}", skipping it')
                continue
            res = conn.execute(f"SELECT * FROM pragma_table_info('{tableName}') WHERE name='{col}'")
            if len(res.fetchall()) == 0:  # If the colun does not exist, we have to add it
                conn.execute(f"ALTER TABLE `{tableName}` ADD `{col}` {colInfo};")

def create_run_backup_table(conn: sqlite3.Connection, tableName: str, runInfoTable: str, logger: logging.Logger):
    res = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}';")
    if len(res.fetchall()) == 0:  # If table does not exist
        create_table_sql = f"CREATE TABLE `{tableName}` (`RunID` INTEGER PRIMARY KEY NOT NULL, `Data` BLOB NOT NULL, FOREIGN KEY (RunID) REFERENCES `{runInfoTable}` (RunID) ON UPDATE CASCADE);"
        conn.execute(create_table_sql)

def make_line_plot(
                    data_df: pandas.DataFrame,
                    file_path: Path,
                    plot_title: str,
                    x_var: str,
                    y_var: str,
                    run_name: str,
                    labels: dict[str, str] = {},
                    full_html:bool = False,
                    subtitle: str = "",
                    extra_title: str = "",
                    x_error: str = None,
                    y_error: str = None,
                    font_size: int = None,
                    color_var: str = None,
                    ):
    if extra_title is None:
        extra_title = ""
    if extra_title != "":
        #extra_title = "<br>" + extra_title
        extra_title = "; " + extra_title

    fig = px.line(
        data_df,
        x=x_var,
        y=y_var,
        error_x=x_error,
        error_y=y_error,
        labels = labels,
        title = "{}<br><sup>{}; Run: {}{}</sup>".format(plot_title, subtitle, run_name, extra_title),
        markers=True,
        #text=var,
        color=color_var,
    )

    if font_size is not None:
        fig.update_layout(
            font=dict(
                #family="Courier New, monospace",
                size=font_size,  # Set the font size here
                #color="RebeccaPurple"
            )
        )

    fig.write_html(
        file_path,
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

def make_series_plot(
                    data_df: pandas.DataFrame,
                    file_path: Path,
                    plot_title: str,
                    var: str,
                    run_name: str,
                    labels: dict[str, str] = {},
                    full_html:bool = False,
                    subtitle: str = "",
                    extra_title: str = "",
                    error: str = None,
                    font_size: int = None,
                    color_var: str = None,
                    ):
    if extra_title is None:
        extra_title = ""
    if extra_title != "":
        #extra_title = "<br>" + extra_title
        extra_title = "; " + extra_title

    if "index" not in labels:
        labels["index"] = "Measurement #"

    fig = px.line(
        data_df,
        x=data_df.index,
        y=var,
        error_x=None,
        error_y=error,
        labels = labels,
        title = "{}<br><sup>{}; Run: {}{}</sup>".format(plot_title, subtitle, run_name, extra_title),
        markers=True,
        #text=var,
        color=color_var,
    )

    if font_size is not None:
        fig.update_layout(
            font=dict(
                #family="Courier New, monospace",
                size=font_size,  # Set the font size here
                #color="RebeccaPurple"
            )
        )

    fig.write_html(
        file_path,
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

if __name__ == "__main__":
    raise RuntimeError("Do not try to run this file, it is not a standalone script. It contains several common utilities used by the other scripts")
