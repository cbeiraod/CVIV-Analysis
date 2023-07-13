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
import pickle

import lip_pps_run_manager as RM

import utilities

def script_main(
                db_path: Path,
                run_name: str,
                observations: str,
                ):
    logger = logging.getLogger('set_run_observation')

    with sqlite3.connect(db_path) as sql_conn:
        utilities.enable_foreign_keys(sql_conn)

        run_info_sql = f"SELECT `RunName`,`path` FROM 'RunInfo' WHERE `RunName`=?;"
        res = sql_conn.execute(run_info_sql, [run_name]).fetchall()

        if len(res) == 0:
            raise RuntimeError(f"Unable to find the run {run_name}.")

        del res
        del run_info_sql

    run_path = db_path.parent / "SetDBObservations"

    with RM.RunManager(run_path.resolve()) as Alan:
        Alan.create_run(raise_error=False)

        previous_observations = Alan.path_directory / "previous_observations.pkl"
        obs_idx = 0
        obs_info = {}
        if previous_observations.exists():
            with open(previous_observations, 'rb') as f:
                obs_info = pickle.load(f)
                if run_name in obs_info:
                    for entry in obs_info[run_name]:
                        if entry is None or entry == False:
                            break
                        obs_idx += 1
                    del entry
                    if obs_idx == len(obs_info[run_name]):
                        obs_info[run_name] += [None]
        if run_name not in obs_info:
            obs_info[run_name] = [None]

        task_name = f'{run_name}_set_observation_{obs_idx}'
        with Alan.handle_task(task_name, drop_old_data=True) as Ada:
            with sqlite3.connect(db_path) as sql_conn:
                utilities.enable_foreign_keys(sql_conn)

                # Get Primary Key
                query = "SELECT `RunID` FROM `RunInfo` WHERE `RunName`=?;"
                res = sql_conn.execute(query, [run_name]).fetchall()
                if len(res) == 0:
                    raise RuntimeError(f"Something is seriously wrong because we could not find the run {run_name} on the second attempt.")
                key = res[0][0]

                # Update the observations
                query = "UPDATE `RunInfo` SET Observations=? WHERE RunID=?;"
                sql_conn.execute(query, [observations, key])

        obs_info[run_name][obs_idx] = Alan.task_ran_successfully(task_name)
        with open(previous_observations, 'wb') as f:
            pickle.dump(obs_info, f, pickle.HIGHEST_PROTOCOL)

def main():
    import argparse

    parser = argparse.ArgumentParser(
                    prog='set_run_observation.py',
                    description='This script set the value in the Observations field in the database for the specified run',
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
        '-r',
        '--run',
        metavar = 'RUN_NAME',
        type = str,
        help = 'The name of the run to process. The run database information will be used to find the run',
        required = True,
        dest = 'run',
    )
    parser.add_argument(
        '-o',
        '--observation',
        metavar = 'TEXT',
        type = str,
        help = 'The observations to be set for the specified run',
        required = True,
        dest = 'observation',
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

    script_main(db_path, args.run, args.observation)

if __name__ == "__main__":
    main()