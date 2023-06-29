import os 

def get_absolute_file_path(file_name, file_directory):
    # Retrieve the absolute path of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the absolute file path for the file in the credentials directory
    file_path = os.path.join(current_dir, "..", file_directory, file_name)

    return file_path