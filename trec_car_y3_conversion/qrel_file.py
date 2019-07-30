import os
from typing import List, Optional, Iterable, Dict

# ---------------------------- Qrel Parsing ----------------------------
from trec_car_y3_conversion.utils import safe_group_by, safe_group_list_by


class QrelLine(object):
    """
    Object representing one line in a qrel file
    """

    def __init__(self, qid: str, doc_id: str, relevance: int) -> None:
        self.qid = qid
        self.doc_id = doc_id
        self.relevance = relevance


    @staticmethod
    def from_line(line:str) -> "QrelLine":
        splits = line.split()
        qid = splits[0]             # Query ID
        doc_id = splits[2]          # Paragraph ID
        relevance = int(splits[3])       # Relevance of paragraph
        return QrelLine(qid=qid, doc_id=doc_id, relevance=relevance)


class QrelFile(object):
    """
    Responsible for reading a single qrelfile, line-by-line, and storing them in QrelLine data classes.
    """

    def __init__(self,  qrel_file:str)-> None:
        self.lines = self.load_qrel_file(qrel_file)


    def load_qrel_file(self,qrel_file) -> List[QrelLine]:
        qrellines = [] # type: List[QrelLine]
        with open(qrel_file) as f:
            for line in f:
                qrel_line = QrelLine.from_line(line)
                qrellines.append(qrel_line)

        return qrellines

    def group_by_squid(self, squids:Iterable[str])->Dict[str, List[QrelLine]]:
        facet_lines = safe_group_by([(line.qid, line) for line in self.lines])

        squid_to_qrel = safe_group_list_by([(squid, facet_lines[facet_id])
                                            for facet_id in facet_lines
                                            for squid in squids
                                            if facet_id.startswith(squid) ])

        return squid_to_qrel





def load_qrels(qrel_file:str)-> QrelFile:
    return QrelFile(qrel_file=qrel_file)
