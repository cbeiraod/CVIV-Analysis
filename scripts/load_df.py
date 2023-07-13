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
import sqlite3
import pandas

import lip_pps_run_manager as RM

import utilities

def load_df_task(Pedro: RM.RunManager, db_path: Path, run_name: str, output_path: Path):
    with Pedro.handle_task("load_df_task", drop_old_data=True) as Lilly:
        with sqlite3.connect(db_path) as sql_conn:
            utilities.enable_foreign_keys(sql_conn)

            run_info_sql = f"SELECT `RunID`,`RunName`,`path`,`type`,`sample`,`pixel row`,`pixel col`,`begin location`,`end location` FROM 'RunInfo' WHERE `RunName`=?;"
            res = sql_conn.execute(run_info_sql, [run_name]).fetchall()
            if len(res) == 0:
                raise RuntimeError(f"Unable to find information in the database for run {run_name}")

            runId = res[0][0]
            run_file_path = Path(res[0][2])
            run_type = utilities.CVIV_Types(res[0][3])
            sample = res[0][4]
            pixel_row = res[0][5]
            pixel_col = res[0][6]
            begin_location = res[0][7]
            end_location = res[0][8]

            if not run_file_path.exists() or not run_file_path.is_file():
                raise RuntimeError(f"Could not find the run file for run {run_name}")

            cols = None
            # TODO: Missing standard IV
            if run_type == utilities.CVIV_Types.IV_Two_Probes:
                cols = ["Bias Voltage [V]", "Total Current [A]", "Pad Current [A]"]
            elif run_type == utilities.CVIV_Types.CV:
                cols = ["Voltage [V]", "Capacitance [F]", "Conductivity [S]", "Bias Voltage [V]", "Pad Current [A]"]
            else:
                raise RuntimeError(f"Columns are not defined for the run type {run_type}")

            df = pandas.read_csv(
                                run_file_path,
                                sep = "\t",
                                names = cols,
                                skiprows = begin_location + 1,
                                nrows = end_location - begin_location - 1,
                                 )

            # df.to_feather(Lilly.path_directory / "data.feather")
            df.to_csv(Lilly.path_directory / "data.csv", index=False)

            print(df)

def script_main(
                db_path: Path,
                run_name: str,
                output_path: Path,
                ):
    logger = logging.getLogger('load_df')

    run_file_path = None
    with sqlite3.connect(db_path) as sql_conn:
        utilities.enable_foreign_keys(sql_conn)

        run_info_sql = f"SELECT `RunName`,`path` FROM 'RunInfo' WHERE `RunName`=?;"
        res = sql_conn.execute(run_info_sql, [run_name]).fetchall()

        if len(res) > 0:
            run_file_path = Path(res[0][1])
            if not run_file_path.exists() or not run_file_path.is_file():
                raise RuntimeError(f"The original run file ({run_file_path}) is no longer available, please check.")
    if run_file_path is None:
        raise RuntimeError(f"Could not find a run file in the run database for run {run_name}")

    with RM.RunManager(output_path / run_name) as William:
        William.create_run(raise_error=True)

        load_df_task(William, db_path, run_name, output_path)

def main():
    import argparse

    parser = argparse.ArgumentParser(
                    prog='load_df.py',
                    description='This script loads the run data into a dataframe for later use',
                    #epilog='Text at the bottom of help'
                    )

    parser.add_argument(
        '-d',
        '--dbPath',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the database directory, where the run database is placed. Default: ./data',
        default = "./data",
        dest = 'db_path',
    )
    parser.add_argument(
        '-o',
        '--outputPath',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the output directory.',
        required = True,
        dest = 'output_path',
    )
    parser.add_argument(
        '-r',
        '--run',
        metavar = 'RUN_NAME',
        type = str,
        help = 'The name of the run to process. The run database information will be used to find the run',
        required = True,
        dest = 'run',
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

    db_path: Path = args.db_path
    if not db_path.exists():
        raise RuntimeError("The database path does not exist")
    db_path = db_path.absolute()
    db_path = db_path / "run_db.sqlite"
    if not db_path.exists() or not db_path.is_file():
        raise RuntimeError("The database file does not exist")

    output_path: Path = args.output_path
    if not output_path.exists() or not output_path.is_dir():
        logging.error("You must define a valid data output path")
        exit(1)
    output_path = output_path.absolute()

    script_main(db_path, args.run, output_path)

if __name__ == "__main__":
    main()