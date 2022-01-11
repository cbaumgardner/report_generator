"""
Entry point for the report generator
"""
import argparse
from src.report_generator import DataImporter, DataTransformer


def main():

    """ Instantiate and use DataImporter and DataTransformer classes."""

    args = parse_args()

    importer = DataImporter()
    df1 = importer.import_file(args.file1)
    df2 = importer.import_file(args.file2)


    transformer = DataTransformer(df1, df2)
    # Files share column names, this will add "_filename" to the columns
    transformer.append_filenames_to_columns(args.file1, args.file2)
    transformer.generate_json(args.join_cols, args.file1_cols, args.file2_cols)


def parse_args():

    """ Set up our arguments, parse and return them. Also creates -h/--help """

    parser = argparse.ArgumentParser(description="""Reads two files, joins them on the specified \
        column and outputs the desired columns to a JSON file.""")

    parser.add_argument(
    "-file1",
    type=str,
    help="First file to read.",
    required=True
    )

    parser.add_argument(
    "-file1_cols",  # name on the CLI - drop the `--` for positional/required parameters
    nargs="*",  # 0 or more values expected => creates a list
    type=str,
    help="Columns to select from file 1.",
    required=True
    )

    parser.add_argument(
    "-file2",
    type=str,
    help="Second file to read.",
    required=True
    )

    parser.add_argument(
    "-file2_cols",
    nargs="*",
    type=str,
    help="Columns to select from file 2.",
    required=True
    )

    parser.add_argument(
    "-join_cols",
    nargs="*",
    type=str,
    help="Columns to join file 1 and file 2 on. Column name must be the same in each file.",
    required=True
    )

    args = parser.parse_args()

    return args


if __name__ == "__main__":

    main()
