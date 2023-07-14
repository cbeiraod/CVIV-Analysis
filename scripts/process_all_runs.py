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

import lip_pps_run_manager as RM

import utilities

from load_df import script_main as load_df
from plot_iv import script_main as plot_iv
from plot_cv import script_main as plot_cv

def script_main(
                db_path: Path,
                output_path: Path,
                reload_data: bool = False,
                font_size: int = 18,
                ):
    logger = logging.getLogger('process_all_runs')

    res = []
    with sqlite3.connect(db_path) as sql_conn:
        utilities.enable_foreign_keys(sql_conn)

        run_info_sql = f"SELECT `RunName` FROM 'RunInfo';"
        res = sql_conn.execute(run_info_sql).fetchall()

    for runInfo in res:
        run_name: str = runInfo[0]

        run_path = output_path / run_name
        already_exists = False
        no_load = False
        if run_path.exists() and run_path.is_dir():
            with RM.RunManager(run_path) as David:
                if David.task_completed("load_df_task"):
                    if reload_data:
                        already_exists = True
                    else:
                        no_load = True
                else:
                    already_exists = True

        if not no_load:
            load_df(
                    db_path=db_path,
                    run_name=run_name,
                    output_path=output_path,
                    already_exists=already_exists,
                    )

        plot_iv(db_path=db_path, run_path=run_path, font_size=font_size)
        plot_cv(db_path=db_path, run_path=run_path, font_size=font_size)


def main():
    import argparse

    parser = argparse.ArgumentParser(
                    prog='plot_iv.py',
                    description='This script plots the IV curve of an IV or CV run',
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
        help = 'Path to the output directory where to store the run directories with the output.',
        required = True,
        dest = 'output_path',
    )
    parser.add_argument(
        '-r',
        '--reload',
        action='store_true',
        help = 'Whether to reload the data of a run in case it has been previously loaded',
        dest = 'reload',
    )
    parser.add_argument(
        '-f',
        '--fontSize',
        metavar = 'SIZE',
        type = int,
        help = 'Font size to use in the plots. Default: 18',
        default = 18,
        dest = 'font_size',
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

    script_main(db_path, output_path, args.reload, args.font_size)

if __name__ == "__main__":
    main()