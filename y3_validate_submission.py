#!/usr/bin/python3
import json
import sys
from typing import Union, List, Dict, Optional
import argparse
import os

from trec_car_y3_conversion.utils import maybe_compressed_open, safe_group_list_by
from trec_car_y3_conversion.y3_data import ValidationPageWarning, ValidationPageError, Page, JsonParsingError, OutlineReader, \
    Paragraph, ValidationIssue

from trec_car_y3_conversion.paragraph_text_collector import ValidationParagraphError, ParagraphTextCollector

default_paragraph_id_file_name="paragraph_ids.txt.xz"

validation_rules = """
VALIDATION RULES for TREC CAR Y3


Minimal Spec Requirements
-------------------------

- all ids (page squid, run id, section path, etc) must be set to non-empty ascii strings.

- additionally: a paragraph_id must be a hexadecimal string of 40 characters that is contained in the paragraphCorpus.cbor

- Paragraphs for a page must be a non-empty list

- The minimal representation of a paragraph is the `paragraph_id`. The para_body element is optional, but if given, it must be correct and agree with the representation in the paragraphCorpus.cbor. Cannot be set of an empty list, instead the entry must not appear in the json.

- A page's `paragraph_origins` are optional, but if given, they must be correct according to the following defition with valid paragraph id and a float-valued `rank_score`. Cannot be set to an empty list, instead must not appear in json.

   - The section_path` must refer to a valid heading id of the page outlines are allowed. These are to be given in the format "squid/heading id". It is strongly recommended to include paragraphs for all headings.

   - Up to 20 paragraphs are allowed per heading. (We strongly encourage to include exactly 20 paragraphs per heading.)

   - The `rank` field is optional, but if given must  must agree with the sort-order of the `rank_score`. Also, the lowest valid number for `rank` is 1 (i.e., highest rank is 1). Ranks must be unique (i.e., no ties).




Further requirements for Y3 submissions
---------------------------------------

- All page squids must start with the proper namespace, i.e., 'tqa2:`. They cannot contain `%20` symbols, because these were only used in Y1 and Y2 -- not in Y3!

- Run ids must not contain more than 15 alpha-numeric characters including "_-.", but cannot start with '.'. (Please include an abbreviation of your team name!)

- Maximal 20 paragraphs can be givem. We strongly encourage to provide exactly 20 paragraphs!

"""

def get_parser():


    parser = argparse.ArgumentParser(description=
                                     """Validate JSON-L submission files for TREC CAR Y3 of populated pages
                                     against an outline-cbor file and paragraph cbor file (or optional, paragraph-id-list).
                                     Optional fields such as "para_body" and "paragraph_origins" can be validated as well.
                                     Strict validation for TREC CAR Y3 squids (stable query unique ids) can be enabled or disabled.
                                     The script provides different validation modes, such as "fail-on-first", "print-json", and "confirm-correct".
                                     Validation errors and warnings will be printed on stdout. If no output is generated, then the file is correct.  
                                     """ )



    parser.add_argument("--print-validation-rules"
                        , help = "Print the list validation rules and exit."
                        , action = "store_true"
                        )


    parser.add_argument("--json-file"
                        , help = "Single Json-lines file CAR Y3 format."
                        )
    parser.add_argument("--json-dir"
                        , help = "Directory of Json-lines file CAR Y3 format."
                        )

    parser.add_argument("--compression"
                        , help = "If compression scheme is not clear from filename, it can be set via this flag. Valid options: gz, xz, bz2 -- otherwise assumed to be uncompressed."
                        )




    parser.add_argument("--outline-cbor"
                        , help = "Path to an outline.cbor file"
                        , required= True
                        )

    parser.add_argument("-k"
                        , help = "Maximum number of paragraphs to pull from each query in a runfile. (Default is 20)"
                        , default = 20
                        , metavar = "INT"
                        )

    parser.add_argument("--check-y3"
                        , help = "Activate strict checks for TREC CAR Y3 submission (including squid ids)."
                        , action = "store_true"
                        )

    parser.add_argument("--check-origins"
                        , help = "Activate strict checks paragraph_origins"
                        , action = "store_true"
                        )


    parser.add_argument("--check-text-from-paragraph-cbor"
                        , help = "If set, loads and checks paragraph text from the paragraph corpus .cbor file. Remark: This check will be time-consuming."
                        )

    parser.add_argument("--check-text-from-paragraph-id-list"
                        , help = "If set, loads and checks paragraph-ids with list (*.txt / *.txt.xz). This list contains one id per line and can be produced with paragraph_id_list.py. (Only in effect when --check-text-from-paragraph-cbor is not set.)"
                        , metavar= "ID-FILE"
                        )


    parser.add_argument("--fail-on-first"
                        , help = "If set, fails on first error. (Otherwise, lists all issues)"
                        , action = "store_true"
                        )

    parser.add_argument("--print-json"
                        , help = "If set, prints the problematic JSON."
                        , action = "store_true"
                        )


    parser.add_argument("--submission-check-y3"
                        , help = "Checks performed during TREC CAR Y3 upload. Equivalent to -k 20 --check-y3 --fail-on-first --check-text-from-paragraph-id-list %s" % default_paragraph_id_file_name
                        , action = "store_true"
                        )

    parser.add_argument("--confirm-correct"
                        , help = "Confirms if the file is correct on stdout. (Otherwise, files are correct when no output is generated on stderr)"
                        , action = "store_true"
                        )



    parsed = parser.parse_args()
    return parsed.__dict__


def run_parse() -> None:
    parsed = get_parser()
    if (parsed['print_validation_rules']):
        print(validation_rules)
        sys.exit(0)


    outlines_cbor_file = parsed["outline_cbor"]  # type: str
    json_dir = parsed["json_dir"]  # type: str
    json_file = parsed["json_file"]  # type: str
    print_json = parsed["print_json"] # type: bool
    compression = parsed["compression"] # type: Optional[str]

    fail_on_first = parsed["fail_on_first"] # type: bool
    top_k = int(parsed["k"]) # type: int
    check_y3 = parsed["check_y3"] # type: bool
    check_origins = parsed["check_origins"] # type: bool
    confirm_correct = parsed["confirm_correct"] # type: bool

    paragraph_cbor_file = parsed["check_text_from_paragraph_cbor"]  # type: Optional[str]
    paragraph_id_file = parsed["check_text_from_paragraph_id_list"]  # type: Optional[str]


    submission_check_y3 = parsed["submission_check_y3"] # type: bool
    if submission_check_y3:
        if not paragraph_id_file:
            paragraph_id_file = default_paragraph_id_file_name
        if not os.path.isfile(paragraph_id_file):
            raise RuntimeError("Paragraph ID file needed but \"%s\" does not exist. Set with filename with --check-text-from-paragraph-id-list or create with \"python3 paragraph_id_list.py --paragraph-cbor CBOR -o %s\"" % (paragraph_id_file, paragraph_id_file))

        top_k = 20
        check_y3 = True
        fail_on_first = True


    page_prototypes = {} # type: Dict[str, Page]
    with open(outlines_cbor_file, 'rb') as f:
        for page in OutlineReader.initialize_pages(f):
            for facet in page.query_facets:
                page_prototypes[facet.facet_id] = page




    def validate_y3(json_loc):
        jsonErrors = [] # type: List[JsonParsingError]
        validationErrors = dict() # type: Dict[str, List[Union[ValidationPageError, ValidationPageWarning]]]
        validationParagraphsErrors = dict() # type: Dict[str, List[ValidationParagraphError]]
        found_squids = {} # type: Dict[str, Page]
        required_squids = {page.squid: page for page in page_prototypes.values()} # type: Dict[str, Page]


        paragraphs_to_validate = {} # type: Dict[str, List[Paragraph]]


        with maybe_compressed_open(json_loc, compression=compression) as f:
            for line in f:
                try:
                    page = Page.from_json(json.loads(line))
                    found_squids[page.squid] = page

                    errs = [] #type: List[ValidationIssue]
                    errs.extend(page.validate_minimal_spec(fail_on_first=fail_on_first))

                    errs.extend(page.validate_paragraph_origins(top_k=top_k, fail_on_first=fail_on_first, must_exist = check_origins))

                    if(check_y3):
                        errs.extend(page.validate_required_y3_spec(top_k=top_k, maxlen_run_id=15, fail_on_first=fail_on_first))
                        errs.extend(page.validate_y3_paragraph_origins(fail_on_first=fail_on_first, must_exist = check_origins))



                    if errs:
                        validationErrors[page.squid] = errs

                    if (fail_on_first):
                            real_errors = [err for err in errs if isinstance(err, ValidationPageError)]
                            if (real_errors):
                                raise real_errors[0]

                    for para in page.paragraphs:
                        if not para.para_id in paragraphs_to_validate:
                            paragraphs_to_validate[para.para_id] = []
                        paragraphs_to_validate[para.para_id].append(para)


                except JsonParsingError as ex:
                    if(fail_on_first):
                        raise ex
                    jsonErrors.append(ex)
                except ValidationPageError as ex:
                    if(fail_on_first):
                        raise ex



        if paragraph_cbor_file is not None:
            collector = ParagraphTextCollector(paragraphs_to_validate)
            errsDict = collector.validate_all_paragraph_text(paragraph_cbor_file=paragraph_cbor_file, fail_on_first=fail_on_first) # type : List[Tuple[str, List[ValidationParagraphError]]]
            validationParagraphsErrors = safe_group_list_by(errsDict)

            if (fail_on_first and errs):
                raise errs[0]

        elif paragraph_id_file is not None:
            with maybe_compressed_open(paragraph_id_file) as f:
                valid_paragraph_ids = {line.strip() for line in f if line.strip()}

                collector = ParagraphTextCollector(paragraphs_to_validate)
                errsDict = collector.validate_all_paragraph_ids(valid_paragraph_ids=valid_paragraph_ids)
                validationParagraphsErrors = safe_group_list_by(errsDict)

                if (fail_on_first and errs):
                    raise errs[0]


        for squid in found_squids.keys() - (required_squids.keys()):
            if squid not in validationErrors:
                validationErrors[squid] = []
            validationErrors[squid].append(ValidationPageError(message ="Page with %s not in the outline file and therefore must not be submitted." % squid, data = found_squids[squid]))

        for squid in required_squids.keys() - (found_squids.keys()):
            if squid not in validationErrors:
                validationErrors[squid] = []
            validationErrors[squid].append(ValidationPageError(message ="Page with %s is missing, but is contained in the outline file. Page with this squid must be submitted." % squid, data = required_squids[squid]))



        if jsonErrors or validationErrors:
            print("Validation errors for input file \'%s\'" % os.path.basename(json_loc), file=sys.stderr)

        for err in jsonErrors:
            print("\n\nFound JSON Format issues for page %s:" % err.get_squid(), file = sys.stderr)

            print(err.get_msg(), file=sys.stderr)
            if print_json:
                print(err.problematic_json(), file = sys.stderr)

        for (squid, errs) in validationErrors.items():
            print("\n\nValidation issues for page %s:" % squid, file = sys.stderr)
            for err in errs:
                print(err.get_msg(), file = sys.stderr)
            if print_json:
                print(errs[0].problematic_json(), file = sys.stderr)

        for (pid, errsList) in validationParagraphsErrors.items():
            print("\n\nValidation issues for paragraph %s:" % pid, file = sys.stderr)
            for err in errsList:
                    print(err.get_msg(), file = sys.stderr)
                    if print_json:
                        print(err.problematic_json(), file = sys.stderr)


        is_correct = not (jsonErrors or validationErrors or validationParagraphsErrors)

        if (confirm_correct and is_correct):
            print("%s is in correct TREC CAR Y3 format." % json_loc)

        return is_correct

    is_correct = True
    if json_dir:
        for json_loc in os.listdir(json_dir):
            is_correct = is_correct and validate_y3(json_loc)

    if json_file:
        is_correct = is_correct and validate_y3(json_file)

    if not is_correct:
        sys.exit(1)

if __name__ == '__main__':
    run_parse()





