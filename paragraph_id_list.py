#!/usr/bin/python3
from typing import  List
import argparse

from trec_car.read_data import iter_paragraphs


def get_parser():
    parser = argparse.ArgumentParser(description="Produce a list of valid paragraph ids from a paragraph-cbor file.")
    parser.add_argument("-o"
                        , help = "Output file"
                        , metavar="FILE"
                        , required= True
                        )

    parser.add_argument("--paragraph-cbor"
                        , help = "If set, loads and checks paragraph text from the paragraph corpus .cbor file. Remark: This check will be time consuming."
                        , metavar = "CBOR"
                        , required= True
                        )



    parsed = parser.parse_args()
    return parsed.__dict__

def create_para_id_list(paragraph_cbor_file:str)->List[str]:
    with open(paragraph_cbor_file, 'rb') as f:
        return [p.para_id for p in iter_paragraphs(f)]

def write_para_id_set(outfile:str, para_ids:List[str])->None:
    with open(outfile, 'w') as f:
        for id in para_ids:
            f.write(id)
            f.write('\n')

def run_parse() -> None:
    parsed = get_parser()

    paragraph_cbor_file = parsed["paragraph_cbor"]  # type: str
    outfile= parsed["o"]  # type: str

    para_ids = create_para_id_list(paragraph_cbor_file)

    write_para_id_set(outfile = outfile, para_ids = list(set(para_ids)))





if __name__ == '__main__':
    run_parse()


