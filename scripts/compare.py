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
import difflib

def pathCount(path: Path):
    subDir_list = []
    file_list = []

    #print(path)
    #print(path.absolute())
    #print(path.resolve())

    for handle in path.iterdir():
        if handle.is_file():
            file_list += [handle.absolute()]
        if handle.is_dir():
            subDir_list += [handle.absolute()]

    return subDir_list, file_list

def main():
    import argparse

    parser = argparse.ArgumentParser(
                    prog='compare.py',
                    description='This script compares the contents of two directories and looks for differences',
                    #epilog='Text at the bottom of help'
                    )

    parser.add_argument(
        '-i',
        '--in',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the input directory to check the files',
        required=True,
        dest = 'in_path',
    )

    parser.add_argument(
        '-c',
        '--cmp',
        metavar = 'PATH',
        type = Path,
        help = 'Path to the directory where to compare the files against',
        required=True,
        dest = 'cmp_path',
    )

    args = parser.parse_args()

    in_path : Path = args.in_path.absolute()
    cmp_path: Path = args.cmp_path.absolute()

    in_dirs,  in_files  = pathCount(in_path)
    cmp_dirs, cmp_files = pathCount(cmp_path)

    print(f'There are {len(in_dirs) } sub-directories and {len(in_files) } files in the input directory')
    print(f'There are {len(cmp_dirs)} sub-directories and {len(cmp_files)} files in the compare directory')

    diff_files = []
    missing_files = []

    for file in in_files:
        file: Path
        file_name = file.name

        if cmp_path/file_name in cmp_files:
            diff_files += [(file, cmp_path/file_name)]
        else:
            missing_files += [file]

    printed = False
    print(f'There are {len(missing_files)} missing files from the compare directory, will now diff the existing {len(diff_files)} files:')
    for in_file, cmp_file in diff_files:
        with open(in_file, 'r') as file1:
            with open(cmp_file, 'r') as file2:
                in_file:Path
                diff = difflib.unified_diff(
                                            file1.readlines(),
                                            file2.readlines(),
                                            fromfile=str(in_file),
                                            tofile=str(cmp_file),
                                            #lineterm='',
                                            )

                diff_lines = []
                for line in diff:
                    diff_lines += [line]

                if len(diff_lines) != 0:
                    print(f'  - The file {in_file.name} does not match ({str(in_file)})')
                    printed = True
    if not printed:
        print('  No differences found')

if __name__ == "__main__":
    main()