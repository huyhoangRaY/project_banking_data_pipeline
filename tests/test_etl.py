import unittest
import pandas as pd
from scripts.etl_process import transform_data

class TestETL(unittest.TestCase):
    def test_transform_data(self):
        data = {"AccountNumber": ["123", "456"], "CustomerName": [" John Doe ", "Jane Doe"], "Balance": [500, 15000], "Currency": ["USD", "EUR"]}
        df = pd.DataFrame(data)
        transformed_df = transform_data(df)
        self.assertEqual(transformed_df.iloc[0]["CustomerCategory"], "Low")
        self.assertEqual(transformed_df.iloc[1]["CustomerCategory"], "High")
        self.assertEqual(transformed_df.iloc[0]["CustomerName"], "JOHN DOE")

if __name__ == "__main__":
    unittest.main()