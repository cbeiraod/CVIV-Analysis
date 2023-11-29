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
import pickle
import pandas
import sqlite3
import shutil

import lip_pps_run_manager as RM

import utilities


def script_main(
                run_path: Path,
                data_path: Path,
                backup_path: Path,
                input_path: Path,
                ):
    logger = logging.getLogger('load_runs')

    with RM.RunManager(run_path.resolve()) as Johnny:
        Johnny.create_run(raise_error=False)

        previous_loads = Johnny.path_directory / "previous_loads.pkl"
        load_idx = 0
        load_info = [None]
        if previous_loads.exists():
            with open(previous_loads, 'rb') as f:
                load_info = pickle.load(f)
                for entry in load_info:
                    if entry is None or entry == False:
                        break
                    load_idx += 1
                if load_idx == len(load_info):
                    load_info += [None]
            # load_idx = 0  # For testing, remove this later

        task_name = f'load_pass_{load_idx}'
        with Johnny.handle_task(task_name, drop_old_data=True) as Carrie:
            sql_path = data_path / 'run_db.sqlite'
            run_list = utilities.find_and_sort_cviv_runs(input_path, logger)

            run_df = pandas.DataFrame(run_list, index=None)

            with sqlite3.connect(sql_path) as sql_conn:
                utilities.enable_foreign_keys(sql_conn)

                res = sql_conn.execute("BEGIN TRANSACTION;")
                try:
                    runInfoTable = f'RunInfo'
                    utilities.create_run_info_table(sql_conn, runInfoTable, run_df.columns, logger)
                    utilities.create_run_backup_table(sql_conn, 'runBackup', runInfoTable, logger)

                    for idx in range(len(run_list)):
                        runInfo = run_list[idx]

                        find_sql = f"SELECT `RunName`,`path`,`name`,`SHA256`,`MD5` FROM `{runInfoTable}` WHERE `name`='{runInfo['name']}';"
                        res = sql_conn.execute(find_sql).fetchall()
                        if len(res) > 0:
                            res = res[0]
                            matches = True
                            if runInfo['path'] != res[1]:
                                print("Path does not match, but this could be due to running from a different computer or user...")
                                #matches = False
                            if runInfo['name'] != res[2]:
                                matches = False
                            if runInfo['SHA256'] != res[3]:
                                matches = False
                            if runInfo['MD5'] != res[4]:
                                matches = False

                            if matches:
                                continue
                            else:
                                raise RuntimeError(f"Had a matching measurement name, but the other parameters do not match... name {runInfo['name']}")

                        count_sql = f"SELECT COUNT(RunName) FROM `{runInfoTable}`"
                        next_idx = sql_conn.execute(count_sql).fetchall()[0][0]

                        column_str = "'RunName'"
                        values_str = "?"
                        values = ['CVIV-Run{:04d}'.format(next_idx)]

                        for key in runInfo:
                            column_str += f",'{key}'"
                            values_str += ", ?"
                            if type(runInfo[key]) == bool:
                                values += [int(runInfo[key])]
                            elif type(runInfo[key]) == utilities.CVIV_Types:
                                values += [int(runInfo[key].value)]
                            else:
                                values += [runInfo[key]]

                        insert_sql = f"INSERT INTO '{runInfoTable}'({column_str}) VALUES({values_str});"
                        sql_conn.execute(insert_sql, values)

                        find_sql = f"SELECT `RunID`,`RunName` FROM `{runInfoTable}` WHERE `SHA256`='{runInfo['SHA256']}';"
                        res = sql_conn.execute(find_sql).fetchall()[0]

                        # Backup the run information
                        Data = None
                        with open(runInfo['path'], 'rb') as file:
                            Data = file.read()
                        insert_sql = f"INSERT INTO 'runBackup'('RunID','Data') VALUES(?, ?);"
                        sql_conn.execute(insert_sql, [res[0], Data])

                        extension = 'dat'
                        if runInfo['type'] == utilities.CVIV_Types.IV or runInfo['type'] == utilities.CVIV_Types.IV_Two_Probes:
                            extension = 'iv'
                        elif runInfo['type'] == utilities.CVIV_Types.CV:
                            extension = 'cv'
                        shutil.copy(runInfo['path'], backup_path / f'{res[1]}.{extension}')
                except:
                    res = sql_conn.execute("ROLLBACK TRANSACTION;")
                    raise
                else:
                    res = sql_conn.execute("COMMIT TRANSACTION;")

        load_info[load_idx] = Johnny.task_ran_successfully(task_name)
        with open(previous_loads, 'wb') as f:
            pickle.dump(load_info, f, pickle.HIGHEST_PROTOCOL)

def main():
    import argparse

    parser = argparse.ArgumentParser(
                    prog='load_runs.py',
                    description='This script loads the run data into the run database if it is not already there',
                    #epilog='Text at the bottom of help'
                    )

    parser.add_argument(
        '-d',
        '--dataPath',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the data directory, where the analysis results are placed. Default: ./data',
        default = "./data",
        dest = 'data_path',
    )
    parser.add_argument(
        '-b',
        '--backupPath',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the data backup directory. If not set, a sub-directory will be created in the data directory.',
        #required = True,
        dest = 'backup_path',
    )
    parser.add_argument(
        '-i',
        '--inputPath',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the data input directory.',
        required = True,
        dest = 'input_path',
    )
    parser.add_argument(
        '-l',
        '--log-level',
        help = 'Set the logging level. Default: WARNING',
        choices = ["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"],
        default = "WARNING",
        dest = 'log_level',
    )
    parser.add_argument(
        '--log-file',
        help = 'If set, the full log will be saved to a file (i.e. the log level is ignored)',
        action = 'store_true',
        dest = 'log_file',
    )

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(filename='logging.log', filemode='w', encoding='utf-8', level=logging.NOTSET)
    else:
        if args.log_level == "CRITICAL":
            logging.basicConfig(level=50)
        elif args.log_level == "ERROR":
            logging.basicConfig(level=40)
        elif args.log_level == "WARNING":
            logging.basicConfig(level=30)
        elif args.log_level == "INFO":
            logging.basicConfig(level=20)
        elif args.log_level == "DEBUG":
            logging.basicConfig(level=10)
        elif args.log_level == "NOTSET":
            logging.basicConfig(level=0)

    data_path: Path = args.data_path
    if not data_path.exists():
        data_path.mkdir()
    data_path = data_path.absolute()

    backup_path: Path = args.backup_path
    # If the backup path is not set:
    if backup_path is None:
        backup_path = data_path / 'backup'
    if not backup_path.exists():
        backup_path.mkdir()
    backup_path = backup_path.absolute()

    input_path: Path = args.input_path
    if not input_path.exists() or not input_path.is_dir():
        logging.error("You must define a valid data input path")
        exit(1)
    input_path = input_path.absolute()

    script_main(data_path / 'Load_Runs', data_path, backup_path, input_path)

if __name__ == "__main__":
    main()