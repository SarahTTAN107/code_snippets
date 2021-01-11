import apache_beam as beam

def my_grep(line, term):
    if line.startswith(term):
        yield line
        
PROJECT = "project-name"
BUCKET = "bucket-name"

def run():
    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=examplejobingcp',
        '--save_main_session',
        '--staging-location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/staging/'.format(BUCKET),
        '--region=us-central1',
        '--runner=DataflowRunner'
    ]
    
    p = beam.Pipeline(argv=argv)
    input = 'gs://{0}/.../input*.csv'.format(BUCKET) # input storage - upload input to this bucket prior to running this file
    output_prefix = 'gs://{0}/.../output'
    searchTerm = 'Vietnam'
    
    #find all lines that contain the search term 
    (p 
     | 'GetFile' >> beam.io.ReadFromText(input) #read all files from input bucket 
     | 'Grep' >> beam.FlatMap(lambda line: my_grep(line, searchTerm))
     | 'Write' >> beam.io.WriteToText(output_prefix)        
    )
    
    p.run()
    
if __name__ == '__main__':
    run()