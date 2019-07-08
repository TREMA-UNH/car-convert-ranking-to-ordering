from typing import List, Dict, Any, Tuple, Union, Callable, Optional, Set

from trec_car.read_data import iter_paragraphs, ParaText, ParaLink
from trec_car_y3_conversion.y3_data import Paragraph, ParBody, ValidationParagraphError, ErrorCollector, ValidationIssue


class ParagraphTextCollector(object):
    """
    Retrieves text from paragraphCorpus.cbor file and adds it to the corresponding paragrpahs
    """
    def __init__(self, paragraphs_to_consider:Dict[str, List[Paragraph]])-> None:
        self.confirmed_pids = {}   # type: Dict[str,bool]
        self.paragraphs_to_consider = paragraphs_to_consider   # type: Dict[str, List[Paragraph]]





    def iterate_paragraphs(self, paragraph_cbor_file, func:Callable[[Paragraph, List[Union[ParaLink, ParaText]]], Any], max_paras:Optional[int]=None)-> List[Tuple[str, Any]]:
        """
        :param paragraph_cbor_file: Location of the paragraphCorpus.cbor file
        """
        processed_paragraphs = 0
        unique_paragraphs_seen = 0
        total = len(self.paragraphs_to_consider)
        result = []
        with open(paragraph_cbor_file, 'rb') as f:
            for p in iter_paragraphs(f):
                processed_paragraphs += 1
                if processed_paragraphs % 100000 == 0:
                    print("(Searching paragraph cbor): {}".format(processed_paragraphs))

                if max_paras and processed_paragraphs >= max_paras:
                    break

                if p.para_id in self.paragraphs_to_consider:
                    for para in self.paragraphs_to_consider[p.para_id]:
                        result.append((p.para_id, func(para, p.bodies)))

                    unique_paragraphs_seen += 1
                    if unique_paragraphs_seen == total:
                        break

        return result



    # ---- updating ----

    def update_all_paragraph_text(self, paragraph_cbor_file)-> None:
        """
        :param paragraph_cbor_file: Location of the paragraphCorpus.cbor file
        """
        self.iterate_paragraphs(paragraph_cbor_file, self.update_paragraph_text)



    def update_paragraph_text(self, p: Paragraph, pbodies:List[Union[ParaLink, ParaText]])-> None:
        """
        :param p: Paragraph that we will be updating
        :param pbodies:
        """
        for para_body in pbodies:
            body = ParBody.convert_para_body_into_parbody(para_body)

            p.add_para_body(body)





    # --- validation ----

    def validate_all_paragraph_ids(self, valid_paragraph_ids:Set[str]) -> List[Tuple[str, List[ValidationIssue]]]:  # List (paraId, List[Errors])
        """
        :param valid_paragraph_ids: Location of the paragraphCorpus.cbor file
        """

        if all (para.para_body == None for list in self.paragraphs_to_consider.values() for para in list):
            return []   # para_bodies are optional, in which case they must be None.


        self.confirmed_pids = {pid:(pid in valid_paragraph_ids) for pid in self.paragraphs_to_consider.keys()}



        errs2 = []  # type: List[Tuple[str, List[ValidationIssue]]]
        missing_pids = {pid for (pid, checked) in self.confirmed_pids.items() if not checked}
        for pid in missing_pids:
            paragraphs = self.paragraphs_to_consider[pid]
            paragraph = paragraphs[0] # type: Paragraph
            validation_paragraph_error = ValidationParagraphError(
                message="Submission must only contain paragraphs from the paragraphCorpus, but paragraph id %s is not contained. Paragraph must be omitted from the submission." % paragraph.para_id,
                data=paragraph)
            errs2.append((paragraph.para_id, [validation_paragraph_error]))

        return errs2


    def validate_all_paragraph_text(self, paragraph_cbor_file, fail_on_first:bool=False) -> List[Tuple[str, List[ValidationIssue]]]:    # List (paraId, List[Errors])
        """
        :param paragraph_cbor_file: Location of the paragraphCorpus.cbor file
        """

        if all (para.para_body == None for list in self.paragraphs_to_consider.values() for para in list):
            return []   # para_bodies are optional, in which case they must be None.


        self.confirmed_pids = {pid:False for pid in self.paragraphs_to_consider.keys()}
        errs = [(key,elist) for (key,elist) in self.iterate_paragraphs(paragraph_cbor_file, self.validate_paragraph_text, max_paras = None) if elist]

        errs2 = []
        missing_pids = {pid for (pid, checked) in self.confirmed_pids.items() if not checked}
        for pid in missing_pids:
            paragraphs = self.paragraphs_to_consider[pid]
            paragraph = paragraphs[0] # type: Paragraph
            validation_paragraph_error = ValidationParagraphError(
                message="No text available from paragraph-cbor for paragraph %s. Paragraph must be omitted from the submission." % paragraph.para_id,
                data=paragraph)
            errs2.append((paragraph.para_id, [validation_paragraph_error]))

        return errs + errs2


    def validate_paragraph_text(self, p: Paragraph, pbodies:List[Union[ParaLink, ParaText]])-> List[ValidationIssue]:
        errs = ErrorCollector()

        if p.para_body == None:
            errs.addParagraphValidationError("Paragraph %s has undefined body (None). Bodies must either be omitted for all paragraphs or correctly populated. (Some paragraphs have para_bodies.)" % p.para_id, p)
        elif p.para_body == []:
            errs.addParagraphValidationError("Paragraph %s has empty body ([]). Bodies must either be omitted for all paragraphs or correctly populated. (Some paragraphs have para_bodies.)" % p.para_id, p)
        else:
            bodies_ref= [ParBody.convert_para_body_into_parbody(para_body) for para_body in pbodies]
            p_ref = Paragraph(p.para_id, bodies_ref)


            if not (len(p.para_body) == len(bodies_ref)):
                errs.addParagraphValidationError("Paragraph Bodies do not match for paragraph %s. "
                                                     "Found %d body fields, but should have %d. "
                                                     "Paragraph %s has the following content in the paragraph.cbor file:\n%s  "
                                                     % (p.para_id, len(p.para_body), len(bodies_ref),
                                                        p.para_id, str(p_ref.to_json())), p)

            else :
                for body1, body2 in zip(p.para_body, bodies_ref):
                    if not (body1 == body2):
                        errs.addParagraphValidationError("Paragraph Bodies do not match for paragraph %s. "
                                                             "Found %s, but should be %s."
                                                             "Paragraph %s has the following content in the paragraph.cbor file:\n%s  "
                                                             " But provided was paragraph: \n%s" % (p.para_id,str(body1.to_json()), str(body2.to_json()),
                                                                                   p.para_id, str(p_ref.to_json()),
                                                                                   str(p.to_json())), p)

            self.confirmed_pids[p.para_id]=True

        return errs.errors

