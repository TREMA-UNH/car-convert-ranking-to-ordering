#!/usr/bin/python3
import sys

from typing import List, Dict, Optional, Iterable, Set, Union

from trec_car_y3_conversion.run_file import RunFile, RunLine
from trec_car_y3_conversion.y3_data import Page, Paragraph, ParagraphOrigin, RunPageKey, OutlineReader

from trec_car_y3_conversion.paragraph_text_collector import  ParagraphTextCollector



class PageFacetCache():
    """
    A page that is in process of being populated. But we have to do some caching and computation before its done (and then turns into a Page).

    """

    def __init__(self, page:Page)->None:
        self.page=page
        self.facet_paragraphs = dict() # type: Optional[Dict[str,List[Paragraph]]]

        self.paragraph_origins = None # type: Optional[List[ParagraphOrigin]]
        # return page


    def add_paragraph_origins(self, origin):
        self.page.add_paragraph_origins(origin)


    def add_facet_paragraph(self, qid:str, paragraph: Paragraph)->None:
        assert qid.startswith(self.page.squid), ( "Query id %s does not belong to this page %s"  % (qid, self.page.squid))
        if self.facet_paragraphs is None:
            self.facet_paragraphs = dict()

        if qid not in self.facet_paragraphs:
            self.facet_paragraphs[qid]=[]
        self.facet_paragraphs[qid].append(paragraph)


    def populate_paragraphs(self, top_k:int, remove_duplicates:bool)->Page:
        """
        In a round-robin fashion, select the top ceil(top_k/num_facets) paragraphs from each ranking, as set through :func:`add_facet_paragraph`.

        However, in cases where a ranking does not have enough paragraphs, or facets are missing, or
        where top_k is not divisible by num_facets and we would have either more or less than top_k paragraphs, it is
        difficult to best fill the `top_k` budget.

        This function will use a round-robin approach, iteratively filling the budget by selecting one paragraph from
        each facet, then looping over facets until the budget is filled. This function will do its best to
        exactly meet the top_k budget. Only when there are not enough retrieved paragraphs (submitted through :func:`add_facet_paragraph`)
        this function will not maximize the budget.

        After determining which paragraphs to select, this function will populate the self.paragraphs field  (and self.pids)
        by concatenating the selected paragaphs from self.facet_paragraphs in the order in which facets appear in the
        outline.

        This function can only be called when self.paragraphs are not set, but facet_paragraphs are available.

        :param top_k:
        :return:
        """
        # if self.paragraphs:
        #     raise RuntimeError("Page %s is already populated with %d paragraphs. Cannot be populated twice!. Did you mean to read the paragraphs or pids field?" % (self.squid, len(self.paragraphs)))
        if not self.facet_paragraphs :
            raise RuntimeError("No facet_paragraphs set for page %s, cannot populate paragraphs. Did you mean to read the paragraphs or pids field?" % self.page.squid)

        facetKeys = self.facet_paragraphs.keys()
        for fk in facetKeys:
            assert fk.startswith(self.page.squid), "Facet of wrong page"


        self.page.paragraphs = []


        did_change = True

        facet_para_list = {} # type: Dict[str, List[Paragraph]]
        seen = set() if remove_duplicates else list() # type: Union[Set[str],List[str]]
        while len(seen) < top_k and did_change:
            did_change = False
            for facet in self.page.query_facets:
                facet_id = facet.facet_id
                if len(seen) < top_k and facet_id in self.facet_paragraphs and self.facet_paragraphs[facet_id]:
                    para = self.facet_paragraphs[facet_id].pop(0)
                    if not (facet_id in facet_para_list):
                        facet_para_list[facet_id] = []
                    if remove_duplicates:
                        if not (para.para_id in seen):
                            facet_para_list[facet_id].append(para)
                            seen.add(para.para_id)
                    else:
                        facet_para_list[facet_id].append(para)
                        seen.append(para.para_id)

                    did_change = True

        for facet in self.page.query_facets:
            facet_id = facet.facet_id
            if facet_id in facet_para_list:
                self.page.paragraphs.extend(facet_para_list[facet_id])

        if len(seen) == 0:
            print ("Warning: No paragraphs for population of page %s" % (self.page.squid), file=sys.stderr)
        elif len(seen) < top_k:
            print ("Warning: page %s could only be populated with %d paragraphs (instead of full budget %d)" % (self.page.squid, len(seen), top_k), file=sys.stderr)
        self.page.pids = {p.para_id for p in self.page.paragraphs}

        return self.page



class ParagraphFiller(object):
    def __init__(self)->None:
        self.paragraphs_to_retrieve = {} # type: Dict[str, List[Paragraph]]


    def register_paragraph(self, paragraph: Paragraph):
        """
        Remember where this paragraph is for later when we have to parse a paragraphCorpus.cbor file.
        We will be adding the parsed text to this paragraph. Since there can be multiple instances of this paragraph
        among queries and runs, they are all stored in a map of lists for later text retrieval.
        """
        key = paragraph.para_id
        if key not in self.paragraphs_to_retrieve:
            self.paragraphs_to_retrieve[key] = []
        self.paragraphs_to_retrieve[key].append(paragraph)


    def retrieve_text(self, paragraph_cbor_file):
        """
        Passes all registered paragraphs to the ParagraphTextCollector for text retrieval.
        :param paragraph_cbor_file: Location to paragraphCorpus.cbor file
        """
        pcollector = ParagraphTextCollector(self.paragraphs_to_retrieve)
        pcollector.update_all_paragraph_text(paragraph_cbor_file)



class RunManager(ParagraphFiller):
    """
    Responsible for all the heavy lifting:
     - Parses a directory full of runfiles.
     - Creates data classes (that can be turned into jsons) based on these runs
    """

    def __init__(self, outline_cbor_file: str) -> None:
        super(RunManager, self).__init__()
        self.pageCaches = {}  # type: Dict[RunPageKey, PageFacetCache]
        self.populated_pages = {}  # type: Dict[RunPageKey, Page]
        self.page_prototypes = {} # type: Dict[str, Page]

        with open(outline_cbor_file, 'rb') as f:
            for page in OutlineReader.initialize_pages(f):
                for facet in page.query_facets:
                    self.page_prototypes[facet.facet_id] = page

            # self.page_prototypes = {facet.facet_id: page for page in OutlineReader.initialize_pages(f) for facet in page.query_facets}




    def convert_run_line(self, run_line: RunLine) -> None:

        if(run_line.qid in self.page_prototypes):   # Ignore other rankings
            page_prototype = self.page_prototypes[run_line.qid]
            squid = page_prototype.squid
            assert run_line.qid.startswith(squid), "fetched wrong page prototype"

            key = RunPageKey(run_name=run_line.run_name, squid=squid)

            # The first time we see a toplevel query for a particular run, we need to initialize a jsonable page
            if key not in self.pageCaches:
                self.pageCaches[key] = PageFacetCache(page = page_prototype.copy_prototype(run_line.run_name))

            pageCache = self.pageCaches[key]
            assert run_line.qid.startswith(pageCache.page.squid), "fetched wrong page"

            # Add paragraph and register this for later (when we retrieve text / links)
            paragraph = Paragraph(para_id=run_line.doc_id)  # create empty paragraph, contents will be loaded later.
            pageCache.add_facet_paragraph(run_line.qid, paragraph)
            assert run_line.qid.startswith(pageCache.page.squid), "adding paragraphs to wrong page"
            # self.register_paragraph(paragraph)

            # Also add which query this paragraph is with respect to
            origin = ParagraphOrigin(
                para_id=run_line.doc_id,
                rank=run_line.rank,
                rank_score=run_line.score,
                section_path=run_line.qid
            )
            pageCache.add_paragraph_origins(origin)




def populate_pages(outlines_cbor_file: str, runs: Iterable[RunFile], top_k: int, remove_duplicates: bool, paragraph_cbor_file: Optional[str]) ->Iterable[Page]:
    run_manager = RunManager(outline_cbor_file=outlines_cbor_file)
    # After parsing run files, convert lines into paragraphs per facet (pageFacetCache)
    for run in runs:
        for run_line in run.runlines:
            run_manager.convert_run_line(run_line)
    # use pageFacetCache to populate the paragraphs field of the underlying page
    run_manager.populated_pages = {key: pageCache.populate_paragraphs(top_k, remove_duplicates)
                                   for key, pageCache in run_manager.pageCaches.items()}

    # if  paragraph text is requested, register all paragraph_ids, then retrieve text form paragraph-cbor file.

    if (paragraph_cbor_file is not None):
        for page in run_manager.populated_pages.values():
            for para in page.paragraphs:
                run_manager.register_paragraph(para)
        run_manager.retrieve_text(paragraph_cbor_file)
    return run_manager.populated_pages.values()


def populate_pages_with_page_runs(outlines_cbor_file: str, runs: Iterable[RunFile], top_k: int, paragraph_cbor_file: Optional[str]) ->Iterable[Page]:
    page_prototypes = {}
    with open(outlines_cbor_file, 'rb') as f:
        for page in OutlineReader.initialize_pages(f):
            page_prototypes[page.squid] = page


    all_pages = []

    # After parsing run files, convert lines into paragraphs per facet (pageFacetCache)
    for run in runs:
        pages = {}  # type: Dict[str,Page]
        for run_line in run.runlines:
            if(run_line.qid in page_prototypes and run_line.rank <= top_k):   # Ignore other rankings
                if run_line.qid not in pages:
                        pages[run_line.qid] = page_prototypes[run_line.qid].copy_prototype(run_line.run_name)
                page_prototype = pages[run_line.qid]

                squid = page_prototype.squid
                assert run_line.qid.startswith(squid), "fetched wrong page prototype"


                # Add paragraph and register this for later (when we retrieve text / links)
                paragraph = Paragraph(para_id=run_line.doc_id)  # create empty paragraph, contents will be loaded later.
                if page_prototype.paragraphs is None:
                    (page_prototype).paragraphs = []
                page_prototype.paragraphs.append(paragraph)

                # Also add which query this paragraph is with respect to
                origin = ParagraphOrigin(
                    para_id=run_line.doc_id,
                    rank=run_line.rank,
                    rank_score=run_line.score,
                    section_path=run_line.qid
                )
                page_prototype.add_paragraph_origins(origin)
        all_pages.extend(pages.values())

    if (paragraph_cbor_file is not None):
        run_manager = ParagraphFiller()
        for page in all_pages:
            for para in page.paragraphs:
                run_manager.register_paragraph(para)
        run_manager.retrieve_text(paragraph_cbor_file)
    return all_pages

