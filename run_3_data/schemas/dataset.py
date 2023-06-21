"""
This module models the schema required
for each dataset type. Currently, it is required only
to distinguish between two items: RAWDataset and ChildDataset which models
the information required for datatiers AOD, MiniAOD and NanoAOD.
"""
from typing import List, Optional


class ChildDataset:
    """
    This class represents the child datasets derived from a RAW dataset.
    They could be datasets for AOD, MiniAOD and NanoAOD datatiers.

    Attributes:
        dataset (str): Models the dataset name following DBS conventions
        events (int): Number of events registered for the dataset
        runs: (list[int]): Number of runs linked to the dataset when it was created
        type (str): Dataset type registered into DBS. For example: VALID, INVALID, PRODUCTION
        campaign (str): Name of the PdmV campaign linked to this dataset.
            For instance, if you check JSON control file (data/years.json)
            some attributes linked to this are: MiniAODv3 or NanoAODv11
        processing_string (str): Processing string desired for a specific era.
            For instance, if you check JSON control file (data/years.json)
            The semantic of this field means, for example, the following:
            For MiniAODv3 campaign, for datasets linked to the era Run2022A, only
            display into the table the dataset with the processing string: 16Jun2023
        output (List[ChildDataset]): List of child datasets. For example, if this object represents
            a AOD dataset, this field will contain the MiniAOD and NanoAOD if they exist.
        prepid (str): Name of the prepid entry for this dataset into Stats2. This is only modeled
            as a compatibility mode due to we are taking all this information directly from
            DBS
        workflow (str): Link to the computing workflow related to the production of this dataset
            This is only modeled for a compatibility mode.

        Also other attributes of interest that are not going to be published into the
        page but are helpful for troubleshooting.

        datatier (str): Dataset datatier: AOD, MiniAOD, NanoAOD
        era: Dataset era. For example: Run2022A
    """

    def __init__(
        self,
        dataset: str,
        events: int,
        runs: List[int],
        type: str,
        campaign: str,
        processing_string: str,
        datatier: str,
        era: str,
        output: List = [],
        prepid: Optional[str] = None,
        workflow: Optional[str] = None,
    ):
        self.dataset = dataset
        self.events = events
        self.runs = runs
        self.type = type
        self.campaign = campaign
        self.processing_string = processing_string
        self.output = output
        self.datatier = datatier
        self.era = era
        self.prepid = prepid
        self.workflow = workflow

    @property
    def dict(self) -> dict:
        child_dataset: List[dict] = [cd.dict for cd in self.output]
        return {
            "dataset": self.dataset,
            "events": self.events,
            "runs": self.runs,
            "type": self.type,
            "campaign": self.campaign,
            "processing_string": self.processing_string,
            "datatier": self.datatier,
            "era": self.era,
            "prepid": self.prepid,
            "workflow": self.workflow,
            "output": child_dataset,
        }


class RAWDataset:
    """
    Models the attributes required for representing
    a RAW dataset into the original schema and also acts as a data object.

    Attributes:
        dataset (str): Models the dataset name following DBS conventions
        events (int): Number of events registered for the dataset
        twiki_runs: (list[int]): Number of validated runs linked to the dataset.
            This attribute is going to be included after Run 3 finishes. It is model
            here as a compatibility mode for Run 2 UL page
        year: (int): Year when the dataset was produced
        runs: (list[int]): Number of runs linked to the dataset when it was created
        output (list[ChildDataset]): List of child datasets for AOD, MiniAOD and NanoAOD
            datatiers linked to this RAW dataset.
    """

    def __init__(
        self,
        dataset: str,
        events: int,
        year: int,
        runs: List[int],
        output: List[ChildDataset] = [],
        twiki_runs: List[int] = [],
    ):
        self.dataset = dataset
        self.events = events
        self.year = year
        self.runs = runs
        self.output = output
        self.twiki_runs = twiki_runs

    @property
    def dict(self) -> dict:
        child_dataset: List[dict] = [cd.dict for cd in self.output]
        return {
            "dataset": self.dataset,
            "events": self.events,
            "year": self.year,
            "runs": self.runs,
            "output": child_dataset,
            "twiki_runs": self.twiki_runs,
        }