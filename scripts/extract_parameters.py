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
import numpy

import lip_pps_run_manager as RM
import plotly.graph_objects as go

import utilities

from scipy.optimize import curve_fit
from scipy.stats import linregress

# nu > 0
def generalised_sigmoid(x, A, K, C, Q, B, M, nu):
    y = A + (K - A)/((C + Q*numpy.exp(-B*(x-M)))**(1/nu))
    return y

# nu > 0
# C = 1, so we can well define the max
def intermediate_sigmoid(x, A, K, Q, B, M, nu):
    y = A + (K - A)/((1 + Q*numpy.exp(-B*(x-M)))**(1/nu))
    return y

# nu > 0
# C = 1, so we can well define the max
# Q = 1, so t=M is half way between A and K
def my_sigmoid(x, A, K, B, M, nu):
    y = A + (K - A)/((1 + numpy.exp(-B*(x-M)))**(1/nu))
    return y

def extract_cv_params(
                        Catarina: RM.TaskManager,
                        base_name: str,
                        data_df: pandas.DataFrame,
                        font_size: int,
                        full_html: bool = False,
                        do_log: bool = True,
                     ):
    import plotly.express as px
    colors = px.colors.qualitative.Plotly
    voltage_col = "Bias Voltage [V]"
    invcap_col = "InverseCSquare"
    left_color = colors[1]
    middle_color = colors[2]
    right_color = colors[3]
    threshold_voltage = 5  # Remove the lowest voltage points since they seem to have some sort of turn-on curve

    current_df = data_df.loc[data_df[voltage_col] > threshold_voltage]
    current_df.reset_index(inplace = True, drop=True)
    min_voltage = current_df[voltage_col].min()
    max_voltage = current_df[voltage_col].max()
    min_invcap = current_df[invcap_col].min()
    max_invcap = current_df[invcap_col].max()

    bidirectional = False
    if True in current_df["Ascending"].unique() and True in current_df["Descending"].unique():
        bidirectional = True

    figure_hlines = []
    figure_vlines = []

    extent = 0.75  # define the middle section as 75% of the invcap
    mid_invcap = (min_invcap + max_invcap)/2
    delta_invcap = max_invcap - min_invcap
    lowmid_invcap = mid_invcap - delta_invcap*extent/2
    highmid_invcap = mid_invcap + delta_invcap*extent/2
    figure_hlines += [{
        "y_val": lowmid_invcap,
        "dash": "dash",
        "color": middle_color,
    },{
        "y_val": highmid_invcap,
        "dash": "dash",
        "color": middle_color,
    }]

    middle_df = current_df.loc[(current_df[invcap_col] > lowmid_invcap) & (current_df[invcap_col] < highmid_invcap)]
    if not bidirectional and len(middle_df) < 3:
        tmp_df = current_df.copy()
        tmp_df["dist"] = (tmp_df[invcap_col] - mid_invcap).abs()
        tmp_df.sort_values(by = "dist", inplace = True)
        middle_df = current_df.iloc[tmp_df.index[:3]]
        del tmp_df
        figure_hlines += [{
            "y_val": middle_df[invcap_col].min(),
            "dash": "dot",
            "color": "red",
        },{
            "y_val": middle_df[invcap_col].max(),
            "dash": "dot",
            "color": "red",
        }]
    elif bidirectional and len(middle_df) < 6:
        tmp_df = current_df.copy()
        tmp_df["dist"] = (tmp_df[invcap_col] - mid_invcap).abs()
        tmp_df.sort_values(by = "dist", inplace = True)
        middle_df = current_df.iloc[tmp_df.index[:6]]
        del tmp_df
        figure_hlines += [{
            "y_val": middle_df[invcap_col].min(),
            "dash": "dot",
            "color": "red",
        },{
            "y_val": middle_df[invcap_col].max(),
            "dash": "dot",
            "color": "red",
        }]

    middle_fit = linregress(x = middle_df[voltage_col], y = middle_df[invcap_col])

    left_edge = (min_invcap - middle_fit.intercept)/middle_fit.slope - 4
    left_df = current_df.loc[current_df[voltage_col] < left_edge]
    left_fit = linregress(x = left_df[voltage_col], y = left_df[invcap_col])

    right_edge = (max_invcap - middle_fit.intercept)/middle_fit.slope + 4
    right_df = current_df.loc[current_df[voltage_col] > right_edge]
    right_fit = linregress(x = right_df[voltage_col], y = right_df[invcap_col])

    figure_vlines += [{
        "x_val": min_voltage,
        "dash": "dashdot",
        "color": left_color,
    },{
        "x_val": (min_invcap - middle_fit.intercept)/middle_fit.slope - 4,
        "dash": "dashdot",
        "color": left_color,
    },{
        "x_val": (max_invcap - middle_fit.intercept)/middle_fit.slope + 4,
        "dash": "dashdot",
        "color": right_color,
    },{
        "x_val": max_voltage,
        "dash": "dashdot",
        "color": right_color,
    }]


    voltage_edges = [
        0,
        (middle_fit.intercept - left_fit.intercept)/(left_fit.slope - middle_fit.slope),
        (right_fit.intercept - middle_fit.intercept)/(middle_fit.slope - right_fit.slope),
        max_voltage,
    ]


    fig = go.Figure()

    for hline in figure_hlines:
        fig.add_hline(
            y = hline["y_val"],
            line_dash = hline["dash"],
            line_color = hline["color"],
        )
    for vline in figure_vlines:
        fig.add_vline(
            x = vline["x_val"],
            line_dash = vline["dash"],
            line_color = vline["color"],
        )

    fig.add_trace(
        go.Scatter(
                    name = "Data",
                    x = data_df[voltage_col],
                    y = data_df[invcap_col],
                    mode = 'markers',
                   )
                  )

    left_X = numpy.linspace(voltage_edges[0], voltage_edges[1], 300)
    left_Y = left_X * left_fit.slope + left_fit.intercept
    fig.add_trace(
        go.Scatter(
                    name = "Left Fit",
                    x = left_X,
                    y = left_Y,
                    mode = 'lines',
                    hoverinfo = "skip",
                    line = dict(color = left_color),
                   )
                  )

    middle_X = numpy.linspace(voltage_edges[1], voltage_edges[2], 300)
    middle_Y = middle_X * middle_fit.slope + middle_fit.intercept
    fig.add_trace(
        go.Scatter(
                    name = "Middle Fit",
                    x = middle_X,
                    y = middle_Y,
                    mode = 'lines',
                    hoverinfo = "skip",
                    line = dict(color = middle_color),
                   )
                  )

    right_X = numpy.linspace(voltage_edges[2], voltage_edges[3], 300)
    right_Y = right_X * right_fit.slope + right_fit.intercept
    fig.add_trace(
        go.Scatter(
                    name = "Right Fit",
                    x = right_X,
                    y = right_Y,
                    mode = 'lines',
                    hoverinfo = "skip",
                    line = dict(color = right_color),
                   )
                  )

    if font_size is not None:
        fig.update_layout(
            font = dict(
                size = font_size,
            )
        )
    fig.update_layout(
        title = "CV Extraction Fit<br><sup>Run: {}</sup>".format(Catarina.run_name)
    )
    fig.update_xaxes(title="Bias Voltage [V]")
    fig.update_yaxes(title="1/C^2")

    file_path = Catarina.task_path / f"{base_name}-ExtractionFit.html"
    fig.write_html(
        file_path,
        full_html = full_html,
        include_plotlyjs = 'cdn',
    )

    if do_log:
        fig.update_yaxes(type="log")

        fig.write_html(
            file_path.parent / (file_path.stem + "_logy.html"),
            full_html = full_html,
            include_plotlyjs = 'cdn',
        )

        fig.update_xaxes(type="log")

        fig.write_html(
            file_path.parent / (file_path.stem + "_log.html"),
            full_html = full_html,
            include_plotlyjs = 'cdn',
        )

    return {
            'gainLayerDepletionVoltage': voltage_edges[1],
            'fullDepletionVoltage': voltage_edges[2],
           }

def extract_parameters_task(
                Joana: RM.RunManager,
                db_path: Path,
                logger: logging.Logger,
                font_size: int = 18,
                 ):
    if Joana.task_completed("load_df_task"):
        with Joana.handle_task("extract_parameters_task", drop_old_data=True) as Catarina:
            with sqlite3.connect(db_path) as sql_conn:
                utilities.enable_foreign_keys(sql_conn)

                run_name = Catarina.run_name

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

                df = pandas.read_csv(Catarina.path_directory / "data.csv")
                fine_df = df.loc[df['Is Coarse'] == False]
                ascending_df = fine_df.loc[fine_df['Ascending'] == True]
                descending_df = fine_df.loc[fine_df['Descending'] == True]

                if len(fine_df) > 0:
                    # Extract IV parameters
                    if run_type == utilities.CVIV_Types.IV_Two_Probes:
                        pass

                    # Extract CV parameters
                    if run_type == utilities.CVIV_Types.CV:
                        #print(fine_df)
                        param_summary = {
                            'tag1' : [],
                            'tag2' : [],
                            'gainLayerDepletionVoltage': [],
                            'fullDepletionVoltage': [],
                        }

                        params = extract_cv_params(
                                            Catarina,
                                            base_name = "allData",
                                            data_df = fine_df,
                                            font_size = font_size,
                                        )
                        for key in param_summary:
                            if key == "tag1":
                                param_summary[key] += ["allData"]
                                continue
                            if key == "tag2":
                                if len(ascending_df) > 3 and len(descending_df) > 3:
                                    param_summary[key] += ["allData"]
                                else:
                                    param_summary[key] += [None]
                                continue
                            param_summary[key] += [params[key]]
                        if len(ascending_df) > 3:
                            params = extract_cv_params(
                                                Catarina,
                                                base_name = "ascending",
                                                data_df = ascending_df,
                                                font_size = font_size,
                                             )
                            for key in param_summary:
                                if key[:3] == "tag":
                                    param_summary[key] += ["ascending"]
                                    continue
                                param_summary[key] += [params[key]]
                        if len(descending_df) > 3:
                            params = extract_cv_params(
                                                Catarina,
                                                base_name = "descending",
                                                data_df = descending_df,
                                                font_size = font_size,
                                             )
                            for key in param_summary:
                                if key[:3] == "tag":
                                    param_summary[key] += ["descending"]
                                    continue
                                param_summary[key] += [params[key]]

                        df = pandas.DataFrame.from_dict(param_summary)
                        df.to_csv(
                            Catarina.path_directory / "extracted_cv.csv",
                            index = False,
                        )


def script_main(
                db_path: Path,
                run_path: Path,
                font_size: int = 18,
                ):
    logger = logging.getLogger('plot_iv')

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

    with RM.RunManager(run_path) as Joana:
        Joana.create_run(raise_error=False)

        extract_parameters_task(Joana, db_path, logger, font_size=font_size)

def main():
    import argparse

    parser = argparse.ArgumentParser(
                    prog='extract_parameters.py',
                    description='This script extracts parameters from the curve, such as the depletion voltage',
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