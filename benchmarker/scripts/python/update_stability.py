import os
import glob
import json
import unittest


PATH = "./results"
REQUIRED_RECALL = .992


class TestResults(unittest.TestCase):

    def setUp(self):
        self.datapoints = []

        for result_filename in glob.glob(os.path.join(PATH, "*.json")):
            with open(os.path.join(os.getcwd(), result_filename), "r") as result_file:
                self.datapoints.append(json.load(result_file))

    def test_max_recall(self):

        rr_env = os.getenv("REQUIRED_RECALL")

        if rr_env:
            required_recall = float(rr_env)
        else:
            required_recall = REQUIRED_RECALL

        for run_iteration in self.datapoints:

            max_recall = max([run_config["recall"] for run_config in run_iteration])
            self.assertTrue(
                max_recall >= required_recall,
                f"need to achieve at least {required_recall} recall, got only {max_recall}",
            )


if __name__ == "__main__":
    unittest.main()