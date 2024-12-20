""" This Python script is designed to manage data files for Power BI reports within a project folder hosted on Amazon S3. The script includes functions to update 
active data files, archive older versions, maintain a specified limit of subfolders within the archive folder, and create new archive folders for each run. 
It leverages the `boto3` library to interact with S3, `pandas` for data manipulation, and robust error handling to ensure smooth operation. 

Functions: 
    - update_active_data(s3, bucket, project_folder, active_folder, file_name, data): 
      Updates the active data file in the specified S3 bucket folder.

    - add_archived_versions(s3, bucket, project_folder, archive_folder, date_folder, versions): 
      Adds new versions of archived data files to the specified S3 bucket folder. 
      
    - monitor_and_maintain_archive_limit(s3, bucket, project_folder, archive_folder, limit=30): 
      Manages the number of subfolders within the archive folder to stay within a specified limit. 
      
    - add_archive_folder(s3, bucket, project_folder, archive_folder, limit, versions): 
      Creates a new archive folder and uploads the archived data files to the specified S3 bucket folder. 
      
      
Dependencies: 
    - pandas 
    - boto3 
    - traceback 
    - datetime


Created by: Charles Ross
Contact: charles.ross@mbakerintl.com
Last updated by: Charles Ross on 11/13/24
    
"""



################    IMPORT PACKAGES    ######################

import pandas as pd
import traceback
from datetime import datetime


################    ADD ACTIVE POWER BI REPORT DATA TO PROJECT FOLDER    ######################

#----------------------------------------------------------------

# Add Active Data to Project Folder
def update_active_data(s3, bucket, project_folder, active_folder, file_name, data):

    """
    Update an existing file in an AWS S3 bucket with new data.

    Parameters:
    -----------
    s3 : boto3.client
        A Boto3 S3 client object.
    bucket : str
        The name of the S3 bucket.
    project_folder : str
        The folder path in the bucket where project files are stored.
    active_folder : str
        The sub-folder path within the project folder where active files are stored.
    file_name : str
        The name of the file to update.
    data : pandas.DataFrame
        The new data to upload, in DataFrame format.

    Returns:
    --------
    None

    Raises:
    -------
    Exception
        If the file does not exist or there are errors during the update process.

    Explanation:
    ------------
    This function updates a specific file in an AWS S3 bucket with new data. It checks if the file exists in the specified folder path. 
    If the file exists, it updates the file with the provided data. If the file does not exist or an error occurs during the update process, 
    an exception is raised to indicate the failure.

    Note:
    -----
    Ensure that the file exists in the specified folder path within the S3 bucket before calling this function. Any errors during the update 
    process will raise an exception.
    """

    #Set Folder Prefix to Active Data Folder
    folder_prefix = project_folder + active_folder + file_name

    # Check if the file exists in the specified folder
    try:
        #Pull Results for File in S3 Bucket
        result = s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix)

        #Check if File Exists in Folder
        file_exists = 'Contents' in result and any(item['Key'] == folder_prefix for item in result['Contents'])

        #If the file exists
        if file_exists:
            try:
                
                # Update the file with a new version
                s3.put_object(Bucket = bucket, Key = folder_prefix, Body = data.to_csv())
            
            except Exception as e:
                raise Exception(f"Error: Failed to Overwrite Active csv file {file_name} in {project_folder}")
        else:
            #If first upload, Place CSV
            s3.put_object(Bucket = bucket, Key = folder_prefix, Body = data.to_csv())
        
    except Exception as e:
        raise Exception(f"Error: Could not update active csv file in {project_folder}: {e}")
    





################    ADD CLEANING VERSIONS TO ARCHIVED S3    ######################

#----------------------------------------------------------------

def add_archive_package(s3, bucket, project_folder, archive_folder, date_folder, archive_package):

    """
    Archive multiple versions of data files in an AWS S3 bucket.

    Parameters:
    -----------
    s3 : boto3.client
        A Boto3 S3 client object.
    bucket : str
        The name of the S3 bucket.
    project_folder : str
        The folder path in the bucket where project files are stored.
    archive_folder : str
        The sub-folder path within the project folder where archived files are stored.
    date_folder : str
        The folder path corresponding to the date of archiving.
    versions : list of dict
        A list of dictionaries, each containing:
        - 'Field' (str or None): The field associated with the version.
        - 'Step' (str): The step or description of the version.
        - 'Result' (pandas.DataFrame): The data to upload, in DataFrame format.

    Returns:
    --------
    None

    Raises:
    -------
    Exception
        If there are errors during the upload process.

    Explanation:
    ------------
    This function archives multiple versions of data files in a specified S3 bucket. Each version is saved with a unique name 
    based on the step count, field, and step description. The function checks and formats the step count, creates the file name, 
    and uploads each version to the appropriate folder path within the S3 bucket. If an error occurs during the upload process, 
    an exception is raised to indicate the failure.

    Note:
    -----
    Ensure that the data in each version's 'Result' field is a DataFrame that can be converted to CSV format. Any errors during 
    the upload process will raise an exception.
    """
    
    #Set Step Count for File Nameing
    step_count = 1

    #Cycle Through Package and Upload to Archive
    try:
        
        #Cycle Through Eeac Key in the Archive
        for section, content in archive_package.items():

            #Load Duplicates Content
            if section == "Duplicates":
                
                #Set Folder Path
                folder_prefix = project_folder + archive_folder + date_folder + "Duplicates.csv"

                #Upload to Folder
                try:
                    s3.put_object(Bucket = bucket, Key = folder_prefix, Body = content.to_csv())
                except Exception as e:
                    raise Exception(f"Error: Failed to Upload Duplicates Entries to S3 {project_folder + archive_folder + date_folder}")


            #Load Removed Content
            elif section == "Data":
                
                #Set Date for Copy
                date = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
                
                #Set Folder Path
                folder_prefix = project_folder + archive_folder + date_folder + "Archived-Data-" + date + ".csv"

                #Upload to Folder
                try:
                    s3.put_object(Bucket = bucket, Key = folder_prefix, Body = content.to_csv())
                except Exception as e:
                    raise Exception(f"Error: Failed to Upload Archived Data to S3 {project_folder + archive_folder + date_folder}")


    except Exception as e:
        raise Exception(f"Error: Failed to Cycle Through RDS Cleaning Versions While Updating to S3.  {e}")
    





################    MANAGE FOLDER LIMIT FOR ARCHIVE FOLDER IN PROJECT    ######################

#----------------------------------------------------------------

def monitor_and_maintain_archive_limit(s3, bucket, project_folder, archive_folder, limit):

    """
    Monitor and maintain the number of archived folders within a specified limit in an AWS S3 bucket.

    Parameters:
    -----------
    s3 : boto3.client
        A Boto3 S3 client object.
    bucket : str
        The name of the S3 bucket.
    project_folder : str
        The folder path in the bucket where project files are stored.
    archive_folder : str
        The sub-folder path within the project folder where archived files are stored.
    limit : int, optional
        The maximum number of archived folders to maintain. Default is 30.

    Returns:
    --------
    None

    Raises:
    -------
    Exception
        If the script is stuck in a while loop or fails to delete excess folders.

    Explanation:
    ------------
    This function monitors the number of sub-folders within a specified archive folder in an AWS S3 bucket. It ensures that the 
    number of archived folders does not exceed the specified limit. If the count exceeds the limit, the function iterates through 
    the sub-folders, identifies the oldest folders based on the modification date of their contents, and deletes the oldest folders 
    until the count is within the limit. If the deletion process fails or the script is stuck in a loop, an exception is raised.

    Note:
    -----
    Ensure that the folders in the archive folder contain contents to determine the oldest folders. Any errors during the deletion 
    process or if the script is stuck in a loop will raise an exception.
    """

    #Set Folder Prefix
    folder_prefix = project_folder + archive_folder

    # List objects within the specified folder
    result = s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix, Delimiter='/')

    #Check if Folders Exist in Result
    if 'CommonPrefixes' in result:
        subfolders = result['CommonPrefixes']   #Find All Sub-Folders within Passed Folder
        folder_count = len(subfolders)   #Store Folder Count

        # Initialize iteration counter
        iteration_counter = 0
        max_iterations = 150  # Set a threshold for maximum iterations

        # Continue deleting folders until the count is within limit
        while folder_count > limit:
            
            #Monitor Iterations of While Loop
            iteration_counter += 1
            if iteration_counter > max_iterations:
                    raise Exception("Error: Script stuck in while loop while cleaning folders to set limit, update process broken until resolved.")
            
            #Set Empty Folder Dates
            folder_dates = []

            #Iterate Through Sub Folders
            for folder in subfolders:
                folder_key = folder['Prefix']
                folder_objects = s3.list_objects_v2(Bucket=bucket, Prefix=folder_key)

                #Find Oldest Object in Folder
                if 'Contents' in folder_objects:
                    oldest_object = min(folder_objects['Contents'], key=lambda x: x['LastModified'])
                    folder_dates.append((folder_key, oldest_object['LastModified']))
                else:
                    raise Exception(f"Error: Contents not found in Folder List in {folder_key}")

            # Sort folders by date and delete the oldest one
            folder_dates.sort(key=lambda x: x[1])
            oldest_folder_key = folder_dates[0][0]
            
            #Delete Excess Folders by Oldest First
            try:
                s3.delete_object(Bucket=bucket, Key=oldest_folder_key)
            
            except Exception as e:
                raise Exception (f"Error: Failed to delete excess folders when cleaning achrive folder for {project_folder}")

            # Update the folder count and subfolders list
            result = s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix, Delimiter='/')
            subfolders = result['CommonPrefixes']
            folder_count = len(subfolders)





################    ADD NEW ARCHIVE FOLDER FOR DATE IN ARCHIVE    ######################

#----------------------------------------------------------------

def add_archive(s3, bucket, project_folder, archive_folder, limit, archive_package):

    """
    Create a new archive folder and upload versions of data files in an AWS S3 bucket while maintaining a specified limit of archived folders.

    Parameters:
    -----------
    s3 : boto3.client
        A Boto3 S3 client object.
    bucket : str
        The name of the S3 bucket.
    project_folder : str
        The folder path in the bucket where project files are stored.
    archive_folder : str
        The sub-folder path within the project folder where archived files are stored.
    limit : int
        The maximum number of archived folders to maintain.
    versions : list of dict
        A list of dictionaries, each containing:
        - 'Field' (str or None): The field associated with the version.
        - 'Step' (str): The step or description of the version.
        - 'Result' (pandas.DataFrame): The data to upload, in DataFrame format.

    Returns:
    --------
    None

    Raises:
    -------
    Exception
        If there are errors during the archiving process or maintaining the archive limit.

    Explanation:
    ------------
    This function creates a new archive folder with a timestamp and uploads multiple versions of data files to this folder in an AWS S3 bucket. 
    Before uploading, it ensures that the number of archived folders does not exceed the specified limit by calling the `monitor_and_maintain_archive_limit` 
    function to clean up excess folders. Each version is saved with a unique name based on the step count, field, and step description. 
    If an error occurs during the archiving process, an exception is raised to indicate the failure.

    Note:
    -----
    Ensure that the data in each version's 'Result' field is a DataFrame that can be converted to CSV format. Any errors during the archiving 
    process or maintaining the archive limit will raise an exception.
    """
    
    #Set Folder Prefix
    folder_prefix = project_folder + archive_folder

    
    #Monitor and Clean Archive Folder
    try:
        monitor_and_maintain_archive_limit(s3 = s3,
                                        bucket = bucket, 
                                        project_folder = project_folder,
                                        archive_folder = archive_folder,
                                        limit = limit)
    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Error: Failed to Clean Archive Folder before Upload") from e


    #Set Folder Name
    date_folder = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '/'

    #Add Date Folder to Archive Folder
    try:
        s3.put_object(Bucket=bucket, Key=folder_prefix + date_folder)
    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Error: Failed to Upload {date_folder} Archive Folder") from e

    #Add Archiveds CSV Files to Folder
    try:
        add_archive_package(s3 = s3,
                            bucket = bucket,
                            project_folder = project_folder,
                            archive_folder = archive_folder, 
                            date_folder = date_folder,
                            archive_package = archive_package)
    except Exception as e:
        print(traceback.print_exc())
        raise Exception(f"Error: Failed to Upload Archive Cleaning Versions CSVs to {date_folder} Archive Folder") from e