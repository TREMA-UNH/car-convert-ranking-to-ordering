import os
from typing import List, Optional



# ---------------------------- Run Parsing ----------------------------

class RunLine(object):
    """
    Object representing one line in a run file
    """

    def __init__(self, qid:str, doc_id:str, rank:int,score:float,run_name:str) -> None:
        self.qid = qid
        self.doc_id = doc_id
        self.rank = rank
        self.score = score
        self.run_name = run_name


    @staticmethod
    def from_line(line:str, run_name: Optional[str] = None) -> "RunLine":
        splits = line.split()
        qid = splits[0]             # Query ID
        doc_id = splits[2]          # Paragraph ID
        rank = int(splits[3])       # Rank of retrieved paragraph
        score = float(splits[4])    # Score of retrieved paragraph
        if run_name is None:
            run_name = splits[5]        # Name of the run
        return RunLine(qid=qid, doc_id=doc_id, rank=rank, score=score, run_name = run_name)


class RunFile(object):
    """
    Responsible for reading a single runfile, line-by-line, and storing them in RunLine data classes.
    """

    def __init__(self,  top_k:int, run_file:str, run_name:Optional[str] = None)-> None:
        self. runlines = [] # type: List[RunLine]
        self.top_k = top_k # type: int
        self.load_run_file(run_file, run_name)

    def load_run_file(self,run_file, run_name: Optional[str]):
        with open(run_file) as f:
            for line in f:
                run_line = RunLine.from_line(line, run_name)
                if (run_line.rank <= self.top_k):
                    self.runlines.append(run_line)


def load_runs(run_dir:Optional[str], run_file:Optional[str], run_name:Optional[str], top_k:int)-> List[RunFile]:
    runs = []  # type: List[RunFile]
    if run_dir is not None:
        for run_loc in os.listdir(run_dir):
            runs.append(RunFile(top_k=top_k, run_file=run_dir + "/" + run_loc))
    if run_file is not None:
        runs.append(RunFile(top_k=top_k, run_file=run_file, run_name=run_name))
    return runs
