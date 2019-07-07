#!/usr/bin/python3
import json
import sys
from typing import Union, List, Dict, Optional, Tuple
import argparse
import os


from trec_car_y3_conversion.y3_data import ValidationWarning, ValidationError, Page, JsonParsingError, OutlineReader, \
    Paragraph, safe_group_by, ValidationIssue, safe_group_list_by

from trec_car_y3_conversion.paragraph_text_collector import ValidationParagraphError, ParagraphTextCollector


def get_parser():
    parser = argparse.ArgumentParser("y3_validate.py")
    parser.add_argument("--outline-cbor"
                        , help = "Path to an outline.cbor file"
                        , required= True
                        )



    parser.add_argument("--y3-file"
                        , help = "Single Json-lines file CAR Y3 format."
                        )
    parser.add_argument("--y3-dir"
                        , help = "Directory of Json-lines file CAR Y3 format."
                        )

    parser.add_argument("--check-text-from-paragraph-cbor"
                        , help = "If set, loads and checks paragraph text from the paragraph corpus .cbor file. Remark: This check will be time consuming."
                        )

    parser.add_argument("--check-text-from-paragraph-id-list"
                        , help = "If set, loads and checks paragraph text from paragraph-id list (produced by paragraph_list.py)."
                        )

    parser.add_argument("--check-y3"
                        , help = "Activate strict checks for TREC CAR Y3 submission"
                        , action = "store_true"
                        )

    parser.add_argument("--quick-check-y3"
                        , help = "Checks performed during TREC CAR Y3 upload"
                        , action = "store_true"
                        )

    parser.add_argument("--check-origins"
                        , help = "Activate strict checks paragraph_origins"
                        , action = "store_true"
                        )

    parser.add_argument("-k"
                        , help = "Maximum number of paragraphs to pull from each query in a runfile. (Default is 25)"
                        , default = 25
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
    json_dir = parsed["y3_dir"]  # type: str
    json_file = parsed["y3_file"]  # type: str
    print_json = parsed["print_json"] # type: bool

    fail_on_first = parsed["fail_on_first"] # type: bool
    top_k = int(parsed["k"]) # type: int
    check_y3 = parsed["check_y3"] # type: bool
    check_origins = parsed["check_origins"] # type: bool

    paragraph_cbor_file = parsed["check_text_from_paragraph_cbor"]  # type: Optional[str]
    paragraph_id_file = parsed["check_text_from_paragraph_id_list"]  # type: Optional[str]


    quick_check_y3 = parsed["quick_check_y3"] # type: bool
    if quick_check_y3:
        top_k = 25
        check_y3 = True


    page_prototypes = {} # type: Dict[str, Page]
    with open(outlines_cbor_file, 'rb') as f:
        for page in OutlineReader.initialize_pages(f):
            for facet in page.query_facets:
                page_prototypes[facet.facet_id] = page




    def validate_y3(json_loc):
        jsonErrors = [] # type: List[JsonParsingError]
        validationErrors = dict() # type: Dict[str, List[Union[ValidationError, ValidationWarning]]]
        validationParagraphsErrors = dict() # type: Dict[str, List[ValidationParagraphError]]
        found_squids = {} # type: Dict[str, Page]
        required_squids = {page.squid: page for page in page_prototypes.values()} # type: Dict[str, Page]


        paragraphs_to_validate = {} # type: Dict[str, List[Paragraph]]

        with open(json_loc,'r') as f:
            for line in f:
                try:
                    page = Page.from_json(json.loads(line))
                    found_squids[page.squid] = page

                    errs = [] #type: List[ValidationIssue]
                    errs.extend(page.validate_minimal_spec())

                    if(check_y3):
                        errs.extend(page.validate_required_y3_spec(top_k=top_k, maxlen_run_id=8))

                    if(check_origins):
                        errs.extend(page.validate_paragraph_origins(top_k=top_k))

                    if(check_y3 and check_origins):
                        errs.extend(page.validate_y3_paragraph_origins())

                    if errs:
                        validationErrors[page.squid] = errs

                    if (fail_on_first):
                            real_errors = [err for err in errs if isinstance(err, ValidationError)]
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
                except ValidationError as ex:
                    if(fail_on_first):
                        raise ex



        if paragraph_cbor_file is not None:
            collector = ParagraphTextCollector(paragraphs_to_validate)
            errsDict = collector.validate_all_paragraph_text(paragraph_cbor_file=paragraph_cbor_file) # type : List[Tuple[str, List[ValidationParagraphError]]]
            validationParagraphsErrors = safe_group_list_by(errsDict)

            if (fail_on_first and errs):
                raise errs[0]

        elif paragraph_id_file is not None:
            with open(paragraph_id_file,'r') as f:
                paragraph_ids = set(f)

                collector = ParagraphTextCollector(paragraphs_to_validate)
                errsDict = collector.validate_all_paragraph_ids(paragraph_ids=paragraph_ids)
                validationParagraphsErrors = safe_group_list_by(errsDict)

                if (fail_on_first and errs):
                    raise errs[0]


        for squid in found_squids.keys() - (required_squids.keys()):
            if squid not in validationErrors:
                validationErrors[squid] = []
            validationErrors[squid].append(ValidationError(message = "Page with %s not in the outline file and therefore must not be submitted." % squid, data = found_squids[squid]))

        for squid in required_squids.keys() - (found_squids.keys()):
            if squid not in validationErrors:
                validationErrors[squid] = []
            validationErrors[squid].append(ValidationError(message = "Page with %s is missing, but is contained in the outline file. Page with this squid must be submitted." % squid, data = required_squids[squid]))



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
            for errs in errsList:
                for err in errs:
                    print(err.get_msg(), file = sys.stderr)
                    if print_json:
                        print(err.problematic_json(), file = sys.stderr)


    if json_dir:
        for json_loc in os.listdir(json_dir):
            validate_y3(json_loc)

    if json_file:
        validate_y3(json_file)


if __name__ == '__main__':
    run_parse()





