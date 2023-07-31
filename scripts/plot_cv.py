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

def plot_cv_task(
                Pedro: RM.RunManager,
                db_path: Path,
                logger: logging.Logger,
                font_size: int = 18,
                 ):
    with sqlite3.connect(db_path) as sql_conn:
        utilities.enable_foreign_keys(sql_conn)

        run_name = Pedro.run_name

        run_info_sql = f"SELECT `RunName`,`type` FROM 'RunInfo' WHERE `RunName`=?;"
        res = sql_conn.execute(run_info_sql, [run_name]).fetchall()
        if len(res) == 0:
            raise RuntimeError(f"Unable to find information in the database for run {run_name}")

        run_type = utilities.CVIV_Types(res[0][1])

        if run_type != utilities.CVIV_Types.CV:
            logger.info(f"Unable to run CV plotting on a run of type {run_type}")
            return

    if Pedro.task_completed("load_df_task"):
        with Pedro.handle_task("plot_cv_task", drop_old_data=True) as Alice:
            with sqlite3.connect(db_path) as sql_conn:
                utilities.enable_foreign_keys(sql_conn)

                run_name = Alice.run_name

                run_info_sql = f"SELECT `RunID`,`RunName`,`path`,`type`,`sample`,`pixel row`,`pixel col`,`begin location`,`end location`,`Observations` FROM 'RunInfo' WHERE `RunName`=?;"
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
                observations = res[0][9]

                df = pandas.read_csv(Alice.path_directory / "data.csv")

                utilities.make_series_plot(
                                            data_df = df,
                                            file_path = Alice.task_path/"Capacitance.html",
                                            plot_title = "<b>Capacitance Over Measurements</b>",
                                            var = "Capacitance [F]",
                                            run_name = Alice.run_name,
                                            subtitle = f"<b>{sample}</b> - Pixel Row <b>{pixel_row}</b> Column <b>{pixel_col}</b>",
                                            extra_title = observations,
                                            font_size = font_size,
                                            do_log = False,
                                        )
                utilities.make_series_plot(
                                            data_df = df,
                                            file_path = Alice.task_path/"Conductivity.html",
                                            plot_title = "<b>Conductivity Over Measurements</b>",
                                            var = "Conductivity [S]",
                                            run_name = Alice.run_name,
                                            subtitle = f"<b>{sample}</b> - Pixel Row <b>{pixel_row}</b> Column <b>{pixel_col}</b>",
                                            extra_title = observations,
                                            font_size = font_size,
                                            do_log = False,
                                        )
                utilities.make_series_plot(
                                            data_df = df,
                                            file_path = Alice.task_path/"Voltage.html",
                                            plot_title = "<b>Voltage Over Measurements</b>",
                                            var = "Bias Voltage [V]",
                                            run_name = Alice.run_name,
                                            subtitle = f"<b>{sample}</b> - Pixel Row <b>{pixel_row}</b> Column <b>{pixel_col}</b>",
                                            extra_title = observations,
                                            font_size = font_size,
                                            do_log = False,
                                        )
                utilities.make_line_plot(
                                            data_df = df,
                                            file_path = Alice.task_path/"CV.html",
                                            plot_title = "<b>CV - Capacitance vs Voltage</b>",
                                            x_var = "Bias Voltage [V]",
                                            y_var = "Capacitance [F]",
                                            run_name = Alice.run_name,
                                            subtitle = f"<b>{sample}</b> - Pixel Row <b>{pixel_row}</b> Column <b>{pixel_col}</b>",
                                            extra_title = observations,
                                            font_size = font_size,
                                        )
                utilities.make_line_plot(
                                            data_df = df,
                                            file_path = Alice.task_path/"GV.html",
                                            plot_title = "<b>GV - Conductivity vs Voltage</b>",
                                            x_var = "Bias Voltage [V]",
                                            y_var = "Conductivity [S]",
                                            run_name = Alice.run_name,
                                            subtitle = f"<b>{sample}</b> - Pixel Row <b>{pixel_row}</b> Column <b>{pixel_col}</b>",
                                            extra_title = observations,
                                            font_size = font_size,
                                        )
                utilities.make_line_plot(
                                            data_df = df,
                                            file_path = Alice.task_path/"GC.html",
                                            plot_title = "<b>GC - Conductivity vs Capacitance</b>",
                                            x_var = "Capacitance [F]",
                                            y_var = "Conductivity [S]",
                                            run_name = Alice.run_name,
                                            subtitle = f"<b>{sample}</b> - Pixel Row <b>{pixel_row}</b> Column <b>{pixel_col}</b>",
                                            extra_title = observations,
                                            font_size = font_size,
                                        )

                utilities.make_series_plot(
                                            data_df = df,
                                            file_path = Alice.task_path/"Computed.html",
                                            plot_title = "<b>1/C^2 Over Measurements</b>",
                                            var = "InverseCSquare",
                                            run_name = Alice.run_name,
                                            subtitle = f"<b>{sample}</b> - Pixel Row <b>{pixel_row}</b> Column <b>{pixel_col}</b>",
                                            extra_title = observations,
                                            font_size = font_size,
                                            do_log = False,
                                            labels = {
                                                "InverseCSquare": "1/C^2"
                                            },
                                        )
                utilities.make_line_plot(
                                            data_df = df,
                                            file_path = Alice.task_path/"ComV.html",
                                            plot_title = "<b>ComV - 1/C^2 vs Voltage</b>",
                                            x_var = "Bias Voltage [V]",
                                            y_var = "InverseCSquare",
                                            run_name = Alice.run_name,
                                            subtitle = f"<b>{sample}</b> - Pixel Row <b>{pixel_row}</b> Column <b>{pixel_col}</b>",
                                            extra_title = observations,
                                            font_size = font_size,
                                            labels = {
                                                "InverseCSquare": "1/C^2"
                                            },
                                        )


def script_main(
                db_path: Path,
                run_path: Path,
                font_size: int = 18,
                ):
    logger = logging.getLogger('plot_cv')

    run_name = run_path.name

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

    with RM.RunManager(run_path) as Fernando:
        Fernando.create_run(raise_error=False)

        plot_cv_task(Fernando, db_path, logger, font_size=font_size)

def main():
    import argparse

    parser = argparse.ArgumentParser(
                    prog='plot_cv.py',
                    description='This script plots the CV curve of a CV run',
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
        '--runPath',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the run directory.',
        required = True,
        dest = 'run_path',
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

    run_path: Path = args.run_path
    if not run_path.exists() or not run_path.is_dir():
        logging.error("You must define a valid data output path")
        exit(1)
    run_path = run_path.absolute()

    script_main(db_path, run_path, args.font_size)

if __name__ == "__main__":
    main()