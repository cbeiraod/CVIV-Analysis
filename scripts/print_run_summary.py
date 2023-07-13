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

import utilities


def script_main(
                db_path: Path,
                ):
    logger = logging.getLogger('print_run_summary')

    print_string = "{runId}: {runName} - {start} : Sample {sample} : Pixel {pixel_row} {pixel_col} : {run_type} : {observations}"

    with sqlite3.connect(db_path) as sql_conn:
        utilities.enable_foreign_keys(sql_conn)

        run_info_sql = f"SELECT `RunID`,`RunName`,`path`,`type`,`sample`,`pixel row`,`pixel col`,`begin location`,`end location`,`Observations`,`start`,`stop` FROM 'RunInfo';"
        res = sql_conn.execute(run_info_sql).fetchall()

        for runInfo in res:
            runDict = {
                "runId": runInfo[0],
                "runName": runInfo[1],
                "run_file_path": Path(runInfo[2]),
                "run_type": utilities.CVIV_Types(runInfo[3]),
                "sample": runInfo[4],
                "pixel_row": runInfo[5],
                "pixel_col": runInfo[6],
                "begin_location": runInfo[7],
                "end_location": runInfo[8],
                "observations": runInfo[9],
                "start" : runInfo[10],
                "stop" : runInfo[11],
            }

            print(print_string.format(**runDict))

def main():
    import argparse

    parser = argparse.ArgumentParser(
                    prog='print_run_summary.py',
                    description='This script prints a summary of all the runs',
                    #epilog='Text at the bottom of help'
                    )

    parser.add_argument(
        '-d',
        '--dbPath',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the data directory, where the analysis results are placed. Default: ./data',
        default = "./data",
        dest = 'db_path',
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

    script_main(db_path)

if __name__ == "__main__":
    main()