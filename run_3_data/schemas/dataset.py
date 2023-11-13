"""
This module models the schema required
for RAW datasets and for matching its sublevel (children) data tiers.
"""
from typing import List, Optional
import pprint
import re

class Stats2Information:
    """
    Data object to store Stats2 information
    for some output datasets.

    Attributes:
        dataset (str): Dataset information to retrieve.
            using the `output_dataset` parameter.
        campaigns (list[str]): List of campaigns the dataset is related
            according to Stats2 records.
        input_dataset (str): Parent dataset used to produce this one.
        workflow (str): ReqMgr2 request that produced this dataset.
        prepid (str): PdmV identifier related to the request that produced
            this dataset. For this context, it is the identifier for the ReReco request
            in PdmV ReReco application.
        processing_string (str): Dataset's processing string.
        raw (dict): Stats2 record for this dataset.
    """
    def __init__(self, dataset: str, raw: dict) -> None:
        self.dataset: str = dataset
        self.raw: dict = raw
        self.campaigns: List[str] = self.raw.get("Campaigns", [])
        self.input_dataset: str = self.raw.get("InputDataset", "")
        self.workflow: str = self.raw.get("RequestName", "")
        self.prepid: str = self.raw.get("PrepID", "")
        self.processing_string: str = self.raw.get("ProcessingString", "")


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
        self.output = sorted(self.output, key=lambda cd: cd.dataset)
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

    def __repr__(self) -> str:
        return pprint.pformat(
            self.dict,
            width=100,
            compact=True,
            depth=100
        )


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
        self.output = sorted(self.output, key=lambda cd: cd.dataset)
        child_dataset: List[dict] = [cd.dict for cd in self.output]

        return {
            "dataset": self.dataset,
            "events": self.events,
            "year": self.year,
            "runs": self.runs,
            "output": child_dataset,
            "twiki_runs": self.twiki_runs,
        }

    def __repr__(self) -> str:
        return pprint.pformat(
            self.dict,
            width=100,
            compact=True,
            depth=100
        )


class DatasetMetadata:
    """
    Models all the attributes that could be extracted from
    the dataset name and determines if the given
    dataset name is valid using a given regex.

    Attributes:
        full_name (str): Original dataset name, e.g: /BTagMu/Run2023C-PromptReco-v4/AOD
        primary_dataset (str): Dataset primary dataset, e.g: BTagMu
        era (str): Dataset era, e.g: Run2023C
        processing_string (str): Dataset's processing string, e.g: PromptReco
        version (str): Dataset's version, e.g: v4
        datatier (str): Dataset's data tier, e.g: AOD
        valid (bool): Determines if the dataset is valid using a predefined regex.
    """
    ATTRIBUTE_REGEX: str = r"^/(\w*)/(Run[0-9]{4}[A-Z]){1}-(\w*)-(v[0-9]{1,2})/([A-Z]*)$"
    SUBVERSION_REGEX: str = r"^(\w*)_(v[0-9]{1,2})$"
    RAW_REGEX: str = r"^/(\w*)/(Run[0-9]{4}[A-Z]){1}-(v[0-9]{1,2})/(RAW)$"
    RAW = re.compile(RAW_REGEX)
    VALIDATOR = re.compile(ATTRIBUTE_REGEX)
    SUBVERSION = re.compile(SUBVERSION_REGEX)

    def __init__(self, name: str) -> None:
        self.full_name: str = name
        self.primary_dataset: str = ""
        self.era: str = ""
        self.processing_string: str = ""
        self.version: str = ""
        self.datatier: str = ""
        self.__valid: bool = False

        # Update the attributes
        self.__build()

    def __build(self) -> None:
        """
        Parses the metadata
        """
        is_raw: bool = self.__build_raw()
        if not is_raw:
            self.__build_non_raw()

    def __build_raw(self) -> bool:
        """
        Checks if the dataset is related to a RAW one
        and parses its data
        """
        components: List[str] = DatasetMetadata.RAW.findall(self.full_name)
        if not components:
            return False
        
        # Parse the data
        primary_ds, era, version, datatier = components[0]
        self.primary_dataset = primary_ds
        self.era = era
        self.version = version
        self.datatier = datatier
        self.__valid = True
        return True

    def __build_non_raw(self) -> None:
        """
        Parse the metadata for a non RAW dataset
        """
        components: List[str] = DatasetMetadata.VALIDATOR.findall(self.full_name)
        if not components:
            return
    
        # Parse the fields
        primary_ds, era, ps, version, datatier = components[0]
        self.primary_dataset = primary_ds
        self.era = era
        self.processing_string = ps
        self.version = version
        self.datatier = datatier
        self.__valid = True

        # Check if the version is overwritten in the PS
        self.__check_ps()

    def __check_ps(self) -> None:
        """
        Check the processing string to determine
        if there is a version tag that overwrites the
        current one.
        """
        components: List[str] = DatasetMetadata.SUBVERSION.findall(self.processing_string)
        if not components:
            return
        
        # Parse the fields
        _, version = components[0]
        self.version = version

    @property
    def valid(self) -> bool:
        return self.__valid
    
    def __repr__(self) -> str:
        repr: str = (
            f"Dataset <valid={self.valid} name={self.full_name} "
            f"primary_dataset={self.primary_dataset} era={self.era} "
            f"processing_string={self.processing_string} version={self.version} "
            f"datatier={self.datatier}>"
        )
        return repr

