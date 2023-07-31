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
import pandas
import re

import lip_pps_run_manager as RM

from plot_iv import plot_iv_task
from plot_cv import plot_cv_task
from compare_runs import compare_runs_task


def script_main(
                db_path: Path,
                run_path: Path,
                font_size: int = 18,
                ):
    logger = logging.getLogger('replot')

    if not run_path.exists() or not run_path.is_dir():
        raise RuntimeError(f"The run path ({run_path}) does not exist or is not a directory")

    run_name = run_path.name

    with RM.RunManager(run_path) as Diogo:
        Diogo.create_run(raise_error=False)

        if Diogo.task_ran_successfully("plot_iv_task"):
            plot_iv_task(
                Pedro = Diogo,
                db_path = db_path,
                logger = logger,
                font_size = font_size,
            )

        if Diogo.task_ran_successfully("plot_cv_task"):
            plot_cv_task(
                Pedro = Diogo,
                db_path = db_path,
                logger = logger,
                font_size = font_size,
            )

        if Diogo.task_ran_successfully("compare_runs"):
            file = Diogo.get_task_path("compare_runs") / "backup.compare_runs.py"
            if not file.exists() or not file.is_file():
                raise RuntimeError("There was a problem with replotting the compare runs, the backup file is missing")

            output_path = None
            plot_legend = None
            output_regex = r"^#   output_path: .+Path\('(.+)'\)$"
            legend_regex = r"^#   plot_legend: '(.+)'$"

            for line in open(file):
                result = re.match(output_regex,line)
                if result is not None:
                    output_path = Path(result.group(1))

                result = re.match(legend_regex,line)
                if result is not None:
                    plot_legend = result.group(1)

            if plot_legend is None or output_path is None:
                raise RuntimeError('Unable to find parameters from script backup')

            run_df = pandas.read_csv(Diogo.path_directory / "compare_runs.csv")

            compare_runs_task(
                Federico = Diogo,
                db_path = db_path,
                output_path = output_path,
                run_df = run_df,
                plot_legend = plot_legend,
                font_size = font_size,
            )

def main():
    import argparse

    parser = argparse.ArgumentParser(
                    prog='replot.py',
                    description='This script re-runs the plotting tasks of a given run',
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
        help = 'Path to the run directory to replot.',
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