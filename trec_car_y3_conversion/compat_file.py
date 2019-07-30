import json
import sys
from typing import List, TextIO


class CompatEntry(object):
    def __init__(self, sectionId:str, y2SectionId:str, y2PageTitle:str, y2Heading:str, pageTitle:str, headings:str,  keywords:List[str])->None:
        self.keywords = keywords
        self.headings = headings
        self.pageTitle = pageTitle
        self.y2Heading = y2Heading
        self.y2PageTitle = y2PageTitle
        self.y2SectionId = y2SectionId
        self.sectionId = sectionId

    @staticmethod
    def from_json(dict) -> "CompatEntry":
        return CompatEntry(sectionId=dict['sectionId']
                           , y2SectionId=dict['y2SectionId']
                           , y2PageTitle=dict['y2PageTitle']
                           , y2Heading=dict['y2Heading']
                           , pageTitle=dict['pageTitle']
                           , headings=dict['headings']
                           , keywords=dict['keywords'])




def load_compat_handle(json_handle:str)->List[CompatEntry]:
    """ Convert a text file in json-lines format into an iterator of CompatEntry objects
    :param json_handle file handle in json-lines
    """
    return [CompatEntry.from_json(elem) for elem  in json.loads(json_handle)]

def load_compat_file(json_file:str)->List[CompatEntry]:
    with open(json_file,'rt') as f:
        return load_compat_handle(" ".join(f.readlines()))