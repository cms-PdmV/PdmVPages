import unittest
from schemas.dataset import ChildDataset, RAWDataset


class TestDatasetSchema(unittest.TestCase):
    """
    This test will check if the structure is correct for
    RAW datasets and its children datatiers: AOD, MiniAOD and NanoAOD

    Attributes:
        nanoaod_dataset (ChildDataset): An instance of a NanoAOD dataset
        miniaod_dataset (ChildDataset): An instance of a MiniAOD dataset
        aod_dataset (ChildDataset): An instance of a AOD dataset
        raw_dataset (RAWDataset): An instance of a RAW dataset
    """

    def set_clean_environment(self) -> None:
        """
        This method prepares the base environment for the test cases
        """
        self.nanoaod_dataset: ChildDataset = ChildDataset(
            dataset="/BTagCSV/Run2016E-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD",
            events=3,
            runs=[],
            type="VALID",
            campaign="MiniAODv2 NanoAODv9",
            processing_string="HIPM_UL2016_MiniAODv2_NanoAODv9-v2",
            datatier="NANOAOD",
            era="Run2016B",
        )
        self.miniaod_dataset_1: ChildDataset = ChildDataset(
            dataset="/BTagCSV/Run2016B-ver1_HIPM_UL2016_MiniAODv2-v1/MINIAOD",
            events=2,
            runs=[],
            type="VALID",
            campaign="MiniAODv2",
            processing_string="HIPM_UL2016_MiniAODv2_NanoAODv9-v2",
            datatier="MINIAOD",
            era="Run2016B",
        )
        self.miniaod_dataset_2: ChildDataset = ChildDataset(
            dataset="/Charmonium/Run2016B-ver1_HIPM_UL2016_MiniAODv2-v1/MINIAOD",
            events=2,
            runs=[],
            type="VALID",
            campaign="MiniAODv2",
            processing_string="HIPM_UL2016_MiniAODv2_NanoAODv9-v2",
            datatier="MINIAOD",
            era="Run2016B",
        )
        self.aod_dataset: ChildDataset = ChildDataset(
            dataset="/BTagCSV/Run2016B-21Feb2020_ver1_UL2016_HIPM-v1/AOD",
            events=1,
            runs=[],
            type="VALID",
            campaign="AOD",
            processing_string="21Feb2020_ver1_UL2016_HIPM",
            datatier="AOD",
            era="Run2016B",
        )
        self.raw_dataset: RAWDataset = RAWDataset(
            dataset="/BTagCSV/Run2016B-v1/RAW", events=0, year=2016, runs=[]
        )

    def setUp(self):
        """
        This method is automatically called for each test and prepares the relationships
        to be validated. It is going to create the following structure
        This test will create the following relationship
        => raw_dataset -> aod_dataset -> miniaod_dataset_1 -> nanoaod_dataset
                                      -> miniaod_dataset_2
        """
        self.set_clean_environment()

        # Set test structure
        self.miniaod_dataset_1.output = [self.nanoaod_dataset]
        self.aod_dataset.output = [self.miniaod_dataset_1, self.miniaod_dataset_2]
        self.raw_dataset.output = [self.aod_dataset]

    def test_check_nanoaod_relationship(self):
        """
        For a MiniAOD dataset, check that NanoAOD datasets are properly stored
        """
        # Case 1: Check that the child dataset is properly stored into the parent
        miniaod_dataset_1: dict = self.miniaod_dataset_1.dict
        child_dataset: dict = miniaod_dataset_1["output"][0]
        self.assertEqual(
            first=child_dataset["dataset"],
            second=self.nanoaod_dataset.dataset,
            msg="The dataset name are different",
        )

        # Case 2: MiniAOD dataset v2 should not have children datasets
        aod_dataset: dict = self.aod_dataset.dict
        miniaod_dataset_2: dict = aod_dataset["output"][1]
        self.assertEqual(
            first=miniaod_dataset_2["output"],
            second=[],
            msg="This dataset should not have any children dataset",
        )

    def test_raw_dataset_relationship(self):
        """
        From RAW dataset, test that you are able to reach the NanoAOD dataset
        stored into the first bifurcation.
        """
        # Case 1: Reach the NanoAOD dataset from the RAW dataset dictionary
        raw_dataset: dict = self.raw_dataset.dict
        nano_aod_dataset: dict = raw_dataset["output"][0]["output"][0]["output"][0]
        self.assertEqual(
            first=nano_aod_dataset["dataset"],
            second=self.nanoaod_dataset.dataset,
            msg="This dataset should be the same",
        )
