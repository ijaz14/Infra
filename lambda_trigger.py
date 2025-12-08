from pathlib import Path
import os, traceback,sys
root_dir = Path(__file__).parent.parent.absolute()
print(root_dir)
sys.path = [str(root_dir)] + sys.path
print(sys.path)
from svt_utilities.s3_utility import upload_s3_file
 
def random_txt_generator():
    import uuid
    folder_path =  r'E:\svt_ops'
    filename = str(uuid.uuid4()) + ".txt"
    full_path = os.path.join(folder_path, filename)
    print(full_path)
    with open(full_path, 'w') as file:
        file.write("This is some random text to trigger lambda function!\n")
 
    bucket_name = 'hdep-lambda-trigger'
    s3_path = f"lambda_trigger/{filename}"
    print(s3_path)
    try:
        uploaded = upload_s3_file(
            file_name=full_path,
            object_name=s3_path,
            metadata={},
            bucket=bucket_name,
        )
    except Exception as e:
       print(traceback.format_exc())
 
        # file.write("You can add more lines as needed.")
 
 
if __name__ == '__main__':
    random_txt_generator()
