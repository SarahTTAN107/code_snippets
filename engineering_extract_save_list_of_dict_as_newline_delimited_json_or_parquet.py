import json
import pyarrow as pa
import pyarrow.parquet as pq


def create_newlined_delimited_json_file(data: dict, file_path: str):
    json_list = [json.dumps(record) for record in data]
    new_data = '\n'.join(json_list)
    with open(file_path, 'w') as f: 
        f.write(new_data)
        
def create_parquet_file(data: dict, file_path: str):
    """save data (originally json from get request) into a parquet file in path /tmp/file-name 

    Args:
        data (dict): get request returned from vainu enrich_data
    """
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)
    


if __name__ == "__main__":
  file_type = "json"
  print("Saving data into a {} file...".format(file_type))
  if file_type == "parquet":
      print("Creating parquet file...")
      create_parquet_file(new_results, no_lead_file_path)
  if file_type == "json":
      create_newlined_delimited_json_file(new_results, no_lead_file_path)
