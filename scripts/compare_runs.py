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
import shutil
import pandas
import datetime

import lip_pps_run_manager as RM

import utilities

def compare_runs_task(
                        Federico: RM.RunManager,
                        db_path: Path,
                        output_path: Path,
                        run_df: pandas.DataFrame,
                        plot_legend: str,
                        font_size: int = 18,
                      ):
    with Federico.handle_task("compare_runs", drop_old_data=True) as Felicity:
        with sqlite3.connect(db_path) as sql_conn:
            df = pandas.DataFrame()
            param_df = pandas.DataFrame()
            for index, row in run_df.iterrows():
                run = row['Runs']
                labels = row['labels']

                run_info = utilities.get_all_run_info(sql_conn, "RunInfo", run)

                this_run_df = pandas.read_csv(output_path / run / "data.csv")
                if run_info["Run Type"] == utilities.CVIV_Types.CV:
                    file = output_path / run / "extracted_cv.csv"
                    if file.exists():
                        this_run_param_df = pandas.read_csv(file)
                    else:
                        this_run_param_df = pandas.DataFrame()
                else:
                    this_run_param_df = pandas.DataFrame()
                if len(this_run_df["Is Coarse"].unique()) > 1:
                    this_run_df = this_run_df.loc[this_run_df["Is Coarse"] == False]

                legend_display = None

                if plot_legend == "DATAFRAME":
                    this_run_df['Legend'] = labels
                    this_run_param_df['Legend'] = labels
                    legend_display = "Category"
                else:
                    if plot_legend == "TEMPERATURE":
                        this_run_df['Legend'] = f'{run_info["Temperature"]} C'
                        this_run_param_df['Legend'] = run_info["Temperature"]
                        legend_display = "Temperature [C]"
                    elif plot_legend == "PIXEL":
                        this_run_df['Legend'] = f'Pixel {run_info["Pixel"]}'
                        this_run_param_df['Legend'] = f'Pixel {run_info["Pixel"]}'
                        legend_display = "Pixel"
                    elif plot_legend == "SAMPLE":
                        this_run_df['Legend'] = f'{run_info["Sample"]}'
                        this_run_param_df['Legend'] = f'{run_info["Sample"]}'
                        legend_display = "Sample"
                    elif plot_legend == "SAMPLEPIXEL":
                        this_run_df['Legend'] = f'{run_info["Sample"]} - Pixel {run_info["Pixel"]}'
                        this_run_param_df['Legend'] = f'{run_info["Sample"]} - Pixel {run_info["Pixel"]}'
                        legend_display = "Sample - Pixel"
                    elif plot_legend == "FREQUENCY":
                        freq = run_info["LCR Frequency"]
                        if freq is None:
                            freq = "NA"
                            this_run_df['Legend'] = f'{freq}'
                            this_run_param_df['Legend'] = None
                        else:
                            freq /= 1000.
                            this_run_df['Legend'] = f'{freq} kHz'
                            this_run_param_df['Legend'] = freq
                        legend_display = "Frequency [kHz]"
                    elif plot_legend == "IRRADIATION":
                        irrad = run_info["Irradiation Flux"]
                        if irrad is None:
                            irrad = 0
                        else:
                            irrad /= 1000.
                        this_run_df['Legend'] = f'{irrad} p/cm^2'
                        this_run_param_df['Legend'] = irrad
                        legend_display = "Irradiation [p/cm^2]"
                    elif plot_legend == "IAVERAGING":
                        this_run_df['Legend'] = f'{run_info["I averaging"]} I Averages'
                        this_run_param_df['Legend'] = run_info["I averaging"]
                        legend_display = "I Averages"
                    elif plot_legend == "VAVERAGING":
                        this_run_df['Legend'] = f'{run_info["V averaging"]} V Averages'
                        this_run_param_df['Legend'] = run_info["V averaging"]
                        legend_display = "V Averages"
                    elif plot_legend == "TIME":
                        this_run_df['Legend'] = datetime.datetime.fromisoformat(run_info["Start Time"])
                        this_run_param_df['Legend'] = datetime.datetime.fromisoformat(run_info["Start Time"])
                        legend_display = "Time"
                    else:
                        raise RuntimeError(f"Unknown type of legend: {plot_legend}")

                df = pandas.concat([df, this_run_df], ignore_index=True)
                param_df = pandas.concat([param_df, this_run_param_df], ignore_index=True)

            subtitle = f'<b>{run_info["Sample"]}</b> - Pixel Row <b>{run_info["Pixel Row"]}</b> Column <b>{run_info["Pixel Column"]}</b> - Temperature: <b>{run_info["Temperature"]} C</b>'
            if plot_legend == "TEMPERATURE":
                subtitle = f'<b>{run_info["Sample"]}</b> - Pixel Row <b>{run_info["Pixel Row"]}</b> Column <b>{run_info["Pixel Column"]}</b>'
            elif plot_legend == "PIXEL":
                subtitle = f'<b>{run_info["Sample"]}</b> - Temperature: <b>{run_info["Temperature"]} C</b>'
            elif plot_legend == "SAMPLE":
                subtitle = f'Pixel Row <b>{run_info["Pixel Row"]}</b> Column <b>{run_info["Pixel Column"]}</b> - Temperature: <b>{run_info["Temperature"]} C</b>'
            elif plot_legend == "SAMPLEPIXEL":
                subtitle = f'Temperature: <b>{run_info["Temperature"]} C</b>'

            utilities.make_line_plot(
                                        data_df = df,
                                        file_path = Felicity.task_path/"IV.html",
                                        plot_title = "<b>IV - Current vs Voltage</b>",
                                        x_var = "Bias Voltage [V]",
                                        y_var = "Pad Current [A]",
                                        run_name = Felicity.run_name,
                                        subtitle = subtitle,
                                        extra_title = "",
                                        font_size = font_size,
                                        color_var = "Legend",
                                    )
            if run_info["Run Type"] == utilities.CVIV_Types.IV_Two_Probes:
                utilities.make_line_plot(
                                            data_df = df,
                                            file_path = Felicity.task_path/"tIV.html",
                                            plot_title = "<b>tIV - Total Current vs Voltage</b>",
                                            x_var = "Bias Voltage [V]",
                                            y_var = "Total Current [A]",
                                            run_name = Felicity.run_name,
                                            subtitle = subtitle,
                                            extra_title = "",
                                            font_size = font_size,
                                            color_var = "Legend",
                                        )
            if run_info["Run Type"] == utilities.CVIV_Types.CV:
                utilities.make_line_plot(
                                            data_df = df,
                                            file_path = Felicity.task_path/"CV.html",
                                            plot_title = "<b>CV - Capacitance vs Voltage</b>",
                                            x_var = "Bias Voltage [V]",
                                            y_var = "Capacitance [F]",
                                            run_name = Felicity.run_name,
                                            subtitle = subtitle,
                                            extra_title = "",
                                            font_size = font_size,
                                            color_var = "Legend",
                                        )
                utilities.make_line_plot(
                                            data_df = df,
                                            file_path = Felicity.task_path/"GV.html",
                                            plot_title = "<b>GV - Conductivity vs Voltage</b>",
                                            x_var = "Bias Voltage [V]",
                                            y_var = "Conductivity [S]",
                                            run_name = Felicity.run_name,
                                            subtitle = subtitle,
                                            extra_title = "",
                                            font_size = font_size,
                                            color_var = "Legend",
                                        )
                utilities.make_line_plot(
                                            data_df = df,
                                            file_path = Felicity.task_path/"GC.html",
                                            plot_title = "<b>GC - Conductivity vs Capacitance</b>",
                                            x_var = "Capacitance [F]",
                                            y_var = "Conductivity [S]",
                                            run_name = Felicity.run_name,
                                            subtitle = subtitle,
                                            extra_title = "",
                                            font_size = font_size,
                                            color_var = "Legend",
                                        )
                utilities.make_line_plot(
                                            data_df = df,
                                            file_path = Felicity.task_path/"ComV.html",
                                            plot_title = "<b>ComV - 1/C^2 vs Voltage</b>",
                                            x_var = "Bias Voltage [V]",
                                            y_var = "InverseCSquare",
                                            run_name = Felicity.run_name,
                                            subtitle = subtitle,
                                            extra_title = "",
                                            font_size = font_size,
                                            color_var = "Legend",
                                            labels = {
                                                "InverseCSquare": "1/C^2"
                                            },
                                        )
                utilities.make_line_plot(
                                            data_df = param_df,
                                            file_path = Felicity.task_path/"fullDepletionVoltage.html",
                                            plot_title = "<b>Full Bulk Depletion Voltage Evolution</b>",
                                            x_var = "Legend",
                                            y_var = "fullDepletionVoltage",
                                            run_name = Felicity.run_name,
                                            subtitle = subtitle,
                                            extra_title = "",
                                            font_size = font_size,
                                            color_var = "tag1",
                                            labels = {
                                                "tag1": "Data",
                                                "fullDepletionVoltage": "V_{fd}",
                                                "Legend": legend_display,
                                            },
                                        )
                utilities.make_line_plot(
                                            data_df = param_df,
                                            file_path = Felicity.task_path/"gainLayerDepletionVoltage.html",
                                            plot_title = "<b>Gain Layer Depletion Voltage Evolution</b>",
                                            x_var = "Legend",
                                            y_var = "gainLayerDepletionVoltage",
                                            run_name = Felicity.run_name,
                                            subtitle = subtitle,
                                            extra_title = "",
                                            font_size = font_size,
                                            color_var = "tag1",
                                            labels = {
                                                "tag1": "Data",
                                                "gainLayerDepletionVoltage": "V_{gl}",
                                                "Legend": legend_display,
                                            },
                                        )

def script_main(
                run_name: str,
                db_path: Path,
                output_path: Path,
                run_file_path: Path,
                plot_legend: str,
                font_size: int = 18,
                ):
    logger = logging.getLogger('compare_runs')

    with RM.RunManager(output_path / run_name) as Xavier:
        Xavier.create_run(raise_error=False)

        shutil.copy(run_file_path, Xavier.path_directory / "compare_runs.csv")

        run_df = pandas.read_csv(Xavier.path_directory / "compare_runs.csv")
        if plot_legend == "FILE":
            plot_legend = "DATAFRAME"

        all_runs_found_in_db = True
        run_types = []
        with sqlite3.connect(db_path) as sql_conn:
            for run in run_df["Runs"]:
                info = utilities.get_run_info(sql_conn, "RunInfo", run, {"type": "Run Type"})
                if len(info) == 0:
                    all_runs_found_in_db = False
                    logger.debug(f"The run {run} does not exist in the database")

                if info["Run Type"] not in run_types:
                    run_types += [info["Run Type"]]
        if not all_runs_found_in_db:
            raise RuntimeError("Not all of the runs in the run file exist in the database")

        all_runs_found_on_disk = True
        for run in run_df["Runs"]:
            run_path = output_path / run
            if not run_path.exists or not run_path.is_dir():
                all_runs_found_on_disk = False
                logger.debug(f"The run {run} has not yet been loaded into a dataframe")
        if not all_runs_found_on_disk:
            raise RuntimeError("Not all of the runs in the run file have been loaded into a dataframe")

        if len(run_types) != 1:
            raise RuntimeError(f"The selected runs span more than 1 run type: {len(run_types)}")

        compare_runs_task(Xavier, db_path, output_path, run_df, plot_legend, font_size)

def main():
    import argparse

    parser = argparse.ArgumentParser(
                    prog='compare_runs.py',
                    description='This script makes plots comparing multiple runs of the same type',
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
        '--runFile',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the file with the list of runs to compare and possible labels. Default: ./compare_runs.csv',
        default = "./compare_runs.csv",
        dest = 'run_file',
    )
    parser.add_argument(
        '-n',
        '--runName',
        metavar = 'NAME',
        type = str,
        help = 'Name to give to this comparison run. The name will be used to create a directory where the contents will be stored',
        required = True,
        dest = 'run_name',
    )
    parser.add_argument(
        '-o',
        '--outputPath',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the output directory where to place the comparison run output and where the run directories can be found',
        required = True,
        dest = 'output_path',
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
        '-p',
        '--plotLegend',
        metavar = 'OPTION',
        type = str,
        help = 'Value to use for the legend of each run. Defaul: FILE',
        choices = ["FILE","TIME","TEMPERATURE","PIXEL","SAMPLE","SAMPLEPIXEL","FREQUENCY","IRRADIATION","IAVERAGING","VAVERAGING"],
        default = "FILE",
        dest = 'plot_legend',
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

    run_file_path: Path = args.run_file
    if not run_file_path.exists() or not run_file_path.is_file():
        logging.error("You must define a valid run file")
        exit(1)
    run_file_path = run_file_path.absolute()

    script_main(args.run_name, db_path, output_path, run_file_path, args.plot_legend, args.font_size)

if __name__ == "__main__":
    main()