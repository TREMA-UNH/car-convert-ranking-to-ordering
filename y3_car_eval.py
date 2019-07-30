#!/usr/bin/python3
import itertools
import os
import argparse
import json

from typing import List, Iterator, Optional, Any, Tuple, Iterable, Dict

import numpy as np

from trec_car import read_data
from trec_car.read_data import iter_pages
from trec_car_y3_conversion.compat_file import load_compat_file
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

    parser.add_argument("--compat"
                        , help = "Compat files for benchmarkY3train to use benchmarkY2test qrel data."
                        )

    parser.add_argument("--max-relevance"
                        , help = "Maximum relevance score possible (according to qrels). If omitted chosen by max value in qrels."
                        )

    parser.add_argument("--gold-pages"
                        , help = "Pages file containing gold (ground truth) articles with the correct paragraph sequence."
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
def relevance_score(para:Paragraph, facets:Optional[List[Tuple[str,int]]], max_possible_relevance:int) -> float:
    if not facets:
        return 0.0
    else:
        relevances = [rel for qid, rel in facets  if rel > 0]
        rel = 0 if not relevances else np.max(relevances)
        return float(rel) / float(max_possible_relevance)

POSITION_METRIC = "positiondistance"
def position_score(para1:Paragraph, positions1:List[int], para2:Paragraph, positions2:List[int], max_penalty:int)->float:
    if not positions1 or not positions2:
        return float(max_penalty)
    else:
        score = min( np.abs(pos1-pos2) for pos1 in positions1 for pos2 in positions2)
        return float(score)



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

    def __init__(self, page:Page, max_possible_relevance:int)->None:
        self.max_possible_relevance = max_possible_relevance
        self.page=page
        self.paragraph_facets = dict() # type: Optional[Dict[str,List[Tuple[str,int]]]]
        self.paragraph_positions = dict() # type: Optional[Dict[str,List[int]]]
        self.paragraph_transitions = dict() # type: Optional[Dict[str,int]]    # paraid1-paraid2 or hashable id

    # ---------------------------


    def add_paragraph_facet(self, qid:str, para_id: str, relevance:int)->None:
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

    def set_paragraph_position_list(self, position_list:Iterator[Tuple[str, int]])->None:
        self.paragraph_positions = safe_group_by(position_list)

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
            score = relevance_score(para, self.paragraph_facets.get(para.para_id), max_possible_relevance= self.max_possible_relevance)
            relevance_scores.append(score)
        return PageEval(squid = page.squid, run_id= page.run_id, metric = RELEVANCE_METRIC, score = np.mean(relevance_scores))

    def eval_position_score(self, page:Page) -> PageEval:
        prev_para = None
        position_scores = [] # type: List[float]
        for para in page.paragraphs:
            if prev_para:
                score = position_score(prev_para, self.paragraph_positions.get(prev_para.para_id), para, self.paragraph_positions.get(para.para_id), max_penalty=1000)  #todo set max_penalty to gold-length
                position_scores.append(score)
            prev_para = para
        return PageEval(squid = page.squid, run_id= page.run_id, metric = POSITION_METRIC, score = np.mean(position_scores))


    def eval_all(self, page:Page)->List[PageEval]:
        return [self.eval_facet_score(page), self.eval_relevance_score(page), self.eval_position_score(page)]


def flat_paragraphs(goldpage:read_data.Page)->List[str]:  # list of paragraph id

    def flat_list(child_list):
        paras = []
        for child in child_list:
            paras.extend(flat_child(child))
        return paras

    def flat_child(gold_child: read_data.PageSkeleton)->List[str]:
        if isinstance(gold_child, read_data.Section):
            return flat_list(gold_child.children)
        elif isinstance(gold_child, read_data.Paragraph):
            return [gold_child.para_id]
        else:
            return []

    return flat_child(goldpage.skeleton)





def eval_main() -> None:
    parsed = get_parser()
    outlines_cbor_file = parsed["outline_cbor"]  # type: str
    run_dir = parsed["run_directory"]  # type: Optional[str]
    run_file = parsed["run_file"]  # type: Optional[str]
    qrels_file = parsed["qrels"]  # type: str
    compat_file = parsed["compat"]  # type: str
    max_possible_relevance = parsed["max_relevance"] # type:int

    gold_pages_file = parsed["gold_pages"]  # type: str


    eval_data = dict() # type: Dict[str, List[PageEval]] # runName
    relevance_cache = dict() # type: Dict[str, PageRelevanceCache]

    compat_y2_to_y3 = {entry.y2SectionId: entry.sectionId for entry in load_compat_file(compat_file)}
    # compat_y3_to_y2 = [(entry.sectionId, entry.y2SectionId) for entry in load_compat_file(compat_file)]

    qrel_data = QrelFile(qrels_file, qid_translation_map= compat_y2_to_y3)

    with open(outlines_cbor_file, 'rb') as f:
        for page in OutlineReader.initialize_pages(f):
            relevance_cache[page.squid] = PageRelevanceCache(page, max_possible_relevance=max_possible_relevance if max_possible_relevance else  qrel_data.max_possible_relevance())

    num_pages = len(relevance_cache)


    qrels_by_squid = qrel_data.group_by_squid(relevance_cache.keys())
    for squid, qrel_lines in qrels_by_squid.items():
        pageCache = relevance_cache[squid]
        for qline in qrel_lines:
            pageCache.add_paragraph_facet(qid = qline.qid, para_id= qline.doc_id, relevance = qline.relevance)

    with open(gold_pages_file, 'rb') as gold_pages_handle:
        for goldpage in iter_pages(gold_pages_handle):
            gold_paragraph_sequence = flat_paragraphs(goldpage)
            relevance_cache[goldpage.page_id].set_paragraph_position_list( zip(gold_paragraph_sequence, range(1, len(gold_paragraph_sequence))))


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





