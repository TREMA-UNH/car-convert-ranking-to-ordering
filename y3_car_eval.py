#!/usr/bin/python3
import itertools
import os
import argparse
import json

from typing import List, Iterator, Optional, Any, Tuple, Iterable, Dict

import numpy as np

from trec_car_y3_conversion.qrel_file import QrelFile
from trec_car_y3_conversion.utils import maybe_compressed_open, safe_group_by
from trec_car_y3_conversion.y3_data import Page, OutlineReader, Paragraph


def get_parser():
    parser = argparse.ArgumentParser(description="""Evaluation for TREC CAR Y3                                                      
                                                 """)
    parser.add_argument("--outline-cbor"
                        , help = "Path to an outline.cbor file"
                        , required= True
                        )

    parser.add_argument("--run-directory"
                        , help = "Path to a directory containing all runfiles to be parsed (uses run name given in trec run files)."
                        )

    parser.add_argument("--run-file"
                        , help = "Single runfiles to be parsed."
                        )

    parser.add_argument("--qrels"
                        , help = "Qrel file that contains the ground truth facet relevance."
                        )


    parsed = parser.parse_args()
    return parsed.__dict__

def group_pages_by_run_id(pages:Iterable[Page]) -> Iterator[Tuple[Any, Iterable[Page]]]:
    def keyfunc(p):
        return p.run_id
    return itertools.groupby(sorted(pages, key=keyfunc), key=keyfunc)




FACET_METRIC = "facet_overlap"
def facet_score(para1:Paragraph, facets1:Optional[List[Tuple[str,int]]], para2:Paragraph, facets2:Optional[List[Tuple[str,int]]])->float:
    if not facets1 or not facets2:
        return 0.0
    else:
        facets1_ = {qid for  qid, rel in facets1 if rel > 0}   #todo include relevance cutoff
        facets2_ = {qid for  qid, rel in facets2 if rel > 0}
        if facets1_.intersection(facets2_):
            return 1.0
        else:
            return 0.0

RELEVANCE_METRIC = "relevance"
def relevance_score(para:Paragraph, facets:Optional[List[Tuple[str,int]]]) -> float:
    if not facets:
        return 0.0
    else:
        rel = np.max([rel for qid, rel in facets  if rel > 0])
        return float(rel)



class PageEval():
    def __init__(self, squid: str, run_id:str, metric:str, score:float):
        self.squid = squid
        self.run_id = run_id
        self.metric = metric
        self.score = score



class PageRelevanceCache():
    """
    A page that is in process of being populated. But we have to do some caching and computation before its done (and then turns into a Page).

    """

    def __init__(self, page:Page)->None:
        self.page=page
        self.paragraph_facets = dict() # type: Optional[Dict[str,List[Tuple[str,int]]]]
        self.paragraph_positions = dict() # type: Optional[Dict[str,List[int]]]
        self.paragraph_transitions = dict() # type: Optional[Dict[str,int]]    # paraid1-paraid2 or hashable id

    # ---------------------------


    def add_paragraph_facet(self, qid:str, para_id: str, relevance:int )->None:
        assert qid.startswith(self.page.squid), ( "Query id %s does not belong to this page %s"  % (qid, self.page.squid))
        if self.paragraph_facets is None:
            self.paragraph_facets = dict() # type: Dict[str,List[Tuple[str,int]]]  # Dict(paraId -> [ (facetId, relevance)])

        if para_id not in self.paragraph_facets:
            self.paragraph_facets[para_id]=[]
        self.paragraph_facets[para_id].append((qid, relevance))

    def add_paragraph_position(self, position:int, para_id: str)->None:
        if self.paragraph_positions is None:
            self.paragraph_positions = dict()

        if para_id not in self.paragraph_positions:
            self.paragraph_positions[para_id]=[]
        self.paragraph_positions[para_id].append(position)


    def add_paragraph_transition(self, transition_id:str, relevance:int)->None:
        if self.paragraph_transitions is None:
            self.paragraph_transitions = dict()

        if not transition_id in self.paragraph_transitions or self.paragraph_transitions[transition_id] < relevance:
            self.paragraph_transitions[transition_id]=relevance

    # ---------------------------




    def eval_facet_score(self, page:Page) -> PageEval:
        prev_para = None
        facet_scores = [] # type: List[float]
        for para in page.paragraphs:
            if prev_para:
                score = facet_score(prev_para, self.paragraph_facets.get(prev_para.para_id), para, self.paragraph_facets.get(para.para_id))
                facet_scores.append(score)

            prev_para = para


        return PageEval(squid = page.squid, run_id= page.run_id, metric = FACET_METRIC, score = np.mean(facet_scores))

    def eval_relevance_score(self, page:Page) -> PageEval:
        relevance_scores = [] # type: List[float]
        for para in page.paragraphs:
            score = relevance_score(para, self.paragraph_facets.get(para.para_id))
            relevance_scores.append(score)


        return PageEval(squid = page.squid, run_id= page.run_id, metric = RELEVANCE_METRIC, score = np.mean(relevance_scores))

    def eval_all(self, page:Page)->List[PageEval]:
        return [self.eval_facet_score(page), self.eval_relevance_score(page)]






def eval_main() -> None:
    parsed = get_parser()
    outlines_cbor_file = parsed["outline_cbor"]  # type: str
    run_dir = parsed["run_directory"]  # type: Optional[str]
    run_file = parsed["run_file"]  # type: Optional[str]
    qrels = parsed["qrels"]  # type: str


    eval_data = dict() # type: dict[str, List[PageEval]] # runName
    relevance_cache = dict() # type: dict[str, PageRelevanceCache]

    with open(outlines_cbor_file, 'rb') as f:
        for page in OutlineReader.initialize_pages(f):
            relevance_cache[page.squid] = PageRelevanceCache(page)

    num_pages = len(relevance_cache)

    qrel_data = QrelFile(qrels)
    qrels_by_squid = qrel_data.group_by_squid(relevance_cache.keys())
    for squid, qrel_lines in qrels_by_squid.items():
        pageCache = relevance_cache[squid]
        for qline in qrel_lines:
            pageCache.add_paragraph_facet(qid = qline.qid, para_id= qline.doc_id, relevance = qline.relevance)


    # todo rundir
    with maybe_compressed_open(run_file) as f:
        for line in f:
            try:
                page = Page.from_json(json.loads(line))
                data = relevance_cache[page.squid].eval_all(page)

                if not page.run_id in eval_data:
                    eval_data[page.run_id] = []
                eval_data[page.run_id].extend(data)
            except Exception as x:
                raise x

    for name, evals in eval_data.items():
        for metric, evals_ in safe_group_by([(eval.metric, eval) for eval in evals]).items():
            scores = [eval.score for eval in evals_]
            meanScore = np.mean(scores)   #type: float
            stdErr = np.std(scores) / np.sqrt(num_pages) #type: float
            print("%s \t %s \t %f +/- %f"% (name, metric, meanScore, stdErr) )


if __name__ == '__main__':
    eval_main()





