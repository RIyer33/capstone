def split_column(value, split_character="-",extract_index=0,output_type="str"):
    """
    Function is used to split the value (column supplied) and return a particular split (based on index supplied)
    and outputs it to a string, float or int
    """
    
    splits = [eval(output_type)(val) for val in value.split(split_character)]
    return splits[extract_index]

