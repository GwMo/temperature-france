#!/usr/bin/env python3

# Consolidate IGN BDALTI files
# IGN forces you to order and download BDALTI by department. This script
# iterates over all department subdirs in the specified dir and collects the
# data files into common directories.

import os
import sys
import argparse
import re

# Parse the target directory from the command line arguments
def parse_args():
    parser = argparse.ArgumentParser(
        description = 'Consolidate IGN BDALTI files from multiple departments'
    )
    parser.add_argument(
        'dir',
        help = 'the directory containing the BDALTI data to consolidate'
    )
    parser.add_argument('-v', '--verbose', action = 'store_true',
        help = 'print changes as they are made')
    parser.add_argument('-n', '--dry-run', action = 'store_true',
        default = False, help = 'perform a trial run without making any changes')
    args = parser.parse_args()

    if os.path.exists(args.dir):
        return args
    else:
        print(f'No such dir: {args.dir}')
        sys.exit(1)

# Check a dir for department subdirs
def list_department_dirs(root):
    # Pattern to match department subdirs
    regex = re.compile('BDALTIV2_2-0_25M_ASC_LAMB93-IGN69_D\d{3}')
    # regex = re.compile('BDALTIV2_2-0_25M_ASC_LAMB93-IGN(69|78C)_D[0-9AB]{3}')

    # List all dirs according to whether they match pattern
    dirs = [d for d in os.listdir(root) if not os.path.isfile(d)]
    keep = [d for d in dirs if regex.match(d)]
    skip = [d for d in dirs if not regex.match(d)]

    # Exit if no department subdirs
    if not any(keep):
        print(f'No department subdirs in {args.dir}')
        sys.exit(0)

    # Report any subdirs that do not match the department subdir pattern
    if any(skip):
        print('Skipping non-department subdirs:')
        for s in skip:
            print('  ' + s)

    return [os.path.join(root, d) for d in keep]

# Create a dictionary mapping file types to common directories
def common_dict(names, root):
    common = {}
    for name in names:
        dirname = 'BDALTIV2_' + name + '_25M_ASC_LAMB93_IGN69_FR'
        common[name] = os.path.join(root, dirname)
    return common

# Move a file from one dir to another
def move(file, current_dir, new_dir):
    if args.verbose or args.dry_run:
        print('  ' + file + " -> " + new_dir)
    if not args.dry_run:
        os.replace(os.path.join(current_dir, file), os.path.join(new_dir, file))

# Move files from a department dir to the appropriate common dir
def move_to_common_dir(file, parent_dir, common_dict):
    name, ext = os.path.splitext(file)
    parent = os.path.basename(parent_dir)

    # DST files
    if parent.startswith('BDALTIV2_DST_25M_ASC_LAMB93_IGN69'):
        move(file, parent_dir, common_dict['DST'])

    # SRC files
    elif parent.startswith('BDALTIV2_SRC_25M_ASC_LAMB93_IGN69'):
        move(file, parent_dir, common_dict['SRC'])

    # MNT files, metadata, and dalles
    elif parent.startswith('BDALTIV2_MNT_25M_ASC_LAMB93_IGN69'):
        if ext == '.asc' or ext == '.tif':
            move(file, parent_dir, common_dict['MNT'])
        elif ext == '.html' or ext == '.xml':
            move(file, parent_dir, common_dict['METADONNEES'])
        elif name == 'dalles':
            # Rename the file to include the department id
            dept = parent.replace('BDALTIV2_MNT_25M_ASC_LAMB93_IGN69_', '')
            new = name + '_' + dept + ext
            if args.verbose or args.dry_run:
                print('  ' + file + " -> " + new)
            if not args.dry_run:
                os.replace(
                    os.path.join(parent_dir, file),
                    os.path.join(parent_dir, new)
                )
                move(new, parent_dir, common_dict['DALLES'])
        elif args.verbose or args.dry_run:
            print('  Skipping ' + file)

    # All other files (md5 + LISEZ-MOI.pdf)
    elif args.verbose or args.dry_run:
        print('  Skipping ' + file)

if __name__ == "__main__":
    # Parse the target dir from the command line args
    args = parse_args()

    # List department dirs
    depts = list_department_dirs(args.dir)

    # Define dirs into which data will be gathered according to file type
    common_dict = common_dict(['DST', 'MNT', 'SRC', 'DALLES', 'METADONNEES'],
                           args.dir)

    # Ensure the common dirs exist
    for key, path in common_dict.items():
        if not os.path.exists(path):
            if args.verbose or args.dry_run:
                print('Creating ' + path)
            if not args.dry_run:
                os.mkdir(path)

    # Walk through the department dirs, moving files to the common dirs
    for dept in depts:
        if args.verbose or args.dry_run:
            print(os.path.basename(dept))
        for dirpath, dirs, files in os.walk(dept, topdown = False):
            for file in files:
                move_to_common_dir(file, dirpath, common_dict)
