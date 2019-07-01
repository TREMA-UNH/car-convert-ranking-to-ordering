#!/usr/bin/python3
import itertools
from typing import Union
import argparse
import os

from trec_car.read_data import iter_paragraphs, ParaText, ParaLink
from trec_car_y3_conversion.y3_data import *



def get_parser():
    parser = argparse.ArgumentParser("y3_validate.py")
    parser.add_argument("--outline-cbor"
                        , help = "Path to an outline.cbor file"
                        , required= True
                        )


    # parser.add_argument("--run-directory"
    #                     , help = "Path to a directory containing all runfiles to be parsed (uses run name given in trec run files)."
    #                     )

    parser.add_argument("--y3-file"
                        , help = "Single Json-lines file CAR format."
                        )
    parser.add_argument("--y3-dir"
                        , help = "Directory of Json-lines file CAR format."
                        )
    #
    # parser.add_argument("--check-text-from-paragraph-cbor"
    #                     , help = "If set, loads and checks paragraph text from the paragraph corpus .cbor file."
    #                     )

    parser.add_argument("-k"
                        , help = "Maximum number of paragraphs to pull from each query in a runfile. (Default is 10)"
                        , default = 10
                        , metavar = "INT"
                        )

    parser.add_argument("--print-json"
                        , help = "If set, prints the problematic JSON."
                        , action = "store_true"
                        )

    parser.add_argument("--fail-on-first"
                        , help = "If set, fails on first error. (Otherwise, lists all issues)"
                        , action = "store_true"
                        )



    parsed = parser.parse_args()
    return parsed.__dict__


def run_parse() -> None:
    parsed = get_parser()
    outlines_cbor_file = parsed["outline_cbor"]  # type: str
    # run_dir = parsed["run_directory"]  # type: Optional[str]
    # run_file = parsed["run_file"]  # type: Opmv jjtional[str]
    # run_name = parsed["run_name"]  # type: Optional[str]
    json_dir = parsed["y3_dir"]  # type: str
    json_file = parsed["y3_file"]  # type: str
    print_json = parsed["print_json"] # type: bool
    fail_on_first = parsed["fail_on_first"] # type: bool

    top_k = int(parsed["k"]) # type: int
    # paragraph_cbor_file = parsed["check_text_from_paragraph_cbor"]  # type: Optional[str]

    def validate_y3(json_loc):

        validationErrors = dict() # type: Dict[str, List[ValidationError]]
        jsonErrors = [] # type: List[JsonParsingError]
        with open(json_loc,'r') as f:
            for line in f:
                try:
                    page = Page.from_json(json.loads(line))
                    errs = []
                    errs.extend(page.validate_minimal_spec())
                    errs.extend(page.validate_minimal_y3_spec(top_k=top_k, maxlen_run_id=8))
                    if errs:
                        validationErrors[page.squid] = errs
                except JsonParsingError as ex:
                    if(fail_on_first):
                        raise ex
                    jsonErrors.append(ex)
                except ValidationError as ex:
                    if(fail_on_first):
                        raise ex

        for err in jsonErrors:
            print("\n\nFound JSON Format issues for squid %s:" % err.get_squid(), file = sys.stderr)

            print(err.get_msg(), file=sys.stderr)
            if print_json:
                print(err.problematic_json(), file = sys.stderr)

        for (squid, errs) in validationErrors.items():
            print("\n\nValidation issues for squid %s:" % squid, file = sys.stderr)
            for err in errs:
                print(err.get_msg(), file = sys.stderr)
            if print_json:
                print(errs[0].problematic_json(), file = sys.stderr)






    if json_dir:
        for json_loc in os.listdir(json_dir):
            validate_y3(json_loc)

    if json_file:
        validate_y3(json_file)



                # page.validate_page_content(top_k, paragraph_cbor_file)


if __name__ == '__main__':
    run_parse()





