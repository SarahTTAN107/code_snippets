def _remove_data_from_dict(data):
    """
    remove highly nested keys that cause failure when running bq load
    these extremely nested keys will be handled to a separate json, while the rest
    of non-troublesome keys will be then uploaded to GCS
    :param data: dict-formated / json object to be cleaned - outcome of get request
    :return: dict - less nested data
    """
    print("Simplifying json file by removing nested arrays from json...")
    for dict_item in data:
        suspicious_key = ['leads', 'social_profiles', 'funding_rounds', 'structure']
        for key in suspicious_key:
            dict_item.pop(key, None)
        # now check if there is an empty {} in each of the keys of the file, which causes a bq load failure
        # (error: unexpected empty struct in structure or something like that)
        for key in dict_item.keys():
            if not bool(dict_item[key]):
                dict_item[key] = None
    return data
