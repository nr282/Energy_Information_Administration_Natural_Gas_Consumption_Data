"""
Module provides tests for EIA API data.

"""

import unittest
from eia_consumption import get_eia_consumption_data_in_pivot_format

class TestEIADataAcquisition(unittest.TestCase):

    def test_eia_data_acquisition_for_residential(self):
        """
        Checks that we can download the EIA data for the three major components:
            1. Residential
            2. Commercial
            3. Electric Power Generation
        """

        start_date = "2019-01-01"
        end_date = "2021-01-01"

        df = get_eia_consumption_data_in_pivot_format(start_date,
                                                     end_date,
                                                     canonical_component_name="Residential",
                                                     create_new_data=True)

        self.assertTrue(len(df) > 0)

    def test_eia_data_acquisition_for_residential(self):
        """
        Checks that we can download the EIA data for the three major components:
            1. Residential
            2. Commercial
            3. Electric Power Generation
        """

        start_date = "2019-01-01"
        end_date = "2021-01-01"

        df = get_eia_consumption_data_in_pivot_format(start_date,
                                                     end_date,
                                                     canonical_component_name="Commercial",
                                                     create_new_data=True)

        self.assertTrue(len(df) > 0)


    def test_eia_data_acquisition_for_electric_power(self):
        """
        Checks that we can download the EIA data for the three major components:
            1. Residential
            2. Commercial
            3. Electric Power Generation
        """

        start_date = "2019-01-01"
        end_date = "2021-01-01"

        df = get_eia_consumption_data_in_pivot_format(start_date,
                                                     end_date,
                                                     canonical_component_name="Electric",
                                                     create_new_data=True)

        self.assertTrue(len(df) > 0)



if __name__ == '__main__':
    unittest.main()