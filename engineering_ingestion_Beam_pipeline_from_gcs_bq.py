import re
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# build data pipeline to read line by line of a csv file in google cloud storage (GCS) to a table called head_usa_name in dataset lake in Big Query (BQ)

# check data structure with a snippet of csv file 
string = ""
with open("data_files/data_files_head_usa_names.csv", "r") as data_file: 
    for line in data_file: 
        string += line


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
    

def run(argv=None):
    """main function to build the pipeline and run it
    """
    
    parser = argparse.ArgumentParser(description="Build and run pipeline - GSC to BQ")
    # command line arguments we expect
    # dest: name of argument
    # default value for input argument is a local file of same directory, but it can also be a csv file in GCS
    parser.add_argument('--input', 
                        dest='input',
                        help='input file to read. This can be a local file or a file in GCS',
                        default='gs://lucky-leaf-260820/data_files/head_usa_names.csv')

    parser.add_argument('--output', 
                        dest='output',
                        help='Output BQ table to write the result to',
                        default='lake.head_usa_name'
                        )

    known_args, pipeline_args = parser.parse_known_args()
    print(known_args)
    print(pipeline_args)
    
    # instantantiate data ingestion object
    data_ingestion = DataIngestion()
    
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p 
    
    | 'Read from file' >> beam.io.ReadFromText(known_args.input,
                                               skip_header_lines=1)
    
    | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
    
    | 'Write to BigQuery' >> beam.io.Write(
         beam.io.BigQuerySink(
             # The table name is a required argument for the BigQuery sink.
             # In this case we use the value passed in from the command line.
             known_args.output,
             # Here we use the simplest way of defining a schema:
             # fieldName:fieldType
             schema='state:STRING,gender:STRING,year:STRING,name:STRING,'
             'number:STRING,created_date:STRING',
             # Creates the table in BigQuery if it does not yet exist.
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             # Deletes all data in the BigQuery table before writing.
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()    
    


if __name__ == "__main__":
    run()
