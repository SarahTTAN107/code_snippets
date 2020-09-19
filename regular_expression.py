# handling each line in csv to dict (before parsing into Big Query)

from google.cloud import storage
import re
import argparse

test_string = "KS,F,1923,Dorothy,654,11/28/2016"

class DataIngestion:    
    def parse_method(self, string_input):    
        """function that transform a single line in a csv file into a dict to be loaded into BQ

        Args:
            string_input (str): 1 line in a csv file that follows this format: 
            state_abbreviation,gender,year,name,count_of_babies,dataset_created_date
            E.g., 1 line to be parsed: KS,F,1923,Dorothy,654,11/28/2016

        Returns:
           dict : dictionary mapping BQ column names as keys corresponding to value parsed from string_input
        """
        
        """
        values = re.split(",", 
                        re.sub('\n', ',',
                                re.sub('\r\n', '',
                                        re.sub('"', '', string_input))))
        """
        
        values = re.split(",", 
                            re.sub('\r\n', '', 
                                re.sub('"', '', string_input)))                  
        
        row = dict(
            zip(['state', 'gender', 'year', 'name', 'number', 'created_date'], values)
        )
        
        return row
