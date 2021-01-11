# this file is used to reconstruct the code in here: git clone https://github.com/GoogleCloudPlatform/training-data-analyst
# local dev version

import apache_beam as beam
import sys

def my_grep(line, term):
    if term in line:
        yield line 
        
if __name__ == '__main__':
    p = beam.Pipeline(argv=sys.argv)
    print(sys.argv)
    input = "./input_data/input*.txt"
    output_prefix = "./output"
    searchTerm = "Vietnam"
    
    #find all lines that contian the search term
    (p
     | 'GetInput' >> beam.io.ReadFromText(input)
     | 'Grep' >> beam.FlatMap(lambda line: my_grep(line, searchTerm) )
     | 'Write' >> beam.io.WriteToText(output_prefix)
     )
    
    p.run()