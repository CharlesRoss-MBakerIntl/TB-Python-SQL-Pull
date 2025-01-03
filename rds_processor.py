"""
This Python script is engineered to execute database queries on an AWS RDS PostgreSQL database, retrieve the resulting data into a Pandas DataFrame, 
perform comprehensive data cleaning operations, and provide a package containing both archived versions of the DataFrame during the cleaning process 
and a final cleaned version. The class leverages Python for data processing and cleaning, utilizing supplementary utility functions from rds_query_utils 
and rds_clean_utils which are included within the same package.

Classes:
    - RDS: 
      Handles database queries, data schema validation, and data cleaning.

      
Dependencies:
    - pandas
    - datetime
    - rds_query_utils
    - rds_clean_utils

Created by: Charles Ross
Contact: charles.ross@mbakerintl.com
Last updated by: Charles Ross on 11/19/2024
"""



################    IMPORT PACKAGES    ######################
import pandas as pd

################    IMPORT QUERY UTILS    ######################
from rds_query_utils import unpack_query
from rds_query_utils import build_schema
from rds_query_utils import build_query
from rds_query_utils import rds_sql_pull





################    RDA TABLE PULL CLASS    ######################

#----------------------------------------------------------------

class RDS:
    
    """
    A class used to handle database queries, data schema validation, and data cleaning for AWS RDS databases.

    Methods:
    --------
    __init__(conn, cursor, query_package=None, auto=True, schema=None, exclude=None, query=None):
        Initializes the RDS class with database connection, cursor, and query details, 
        and sets up the schema and cleaning lists.

    check_schema(df):
        Validates if the DataFrame matches the expected schema defined in the class.

    clean_table(data):
        Cleans the provided DataFrame based on predefined cleaning steps and maintains a log of the cleaning versions.

    query_to_df(clean=True):
        Executes the stored SQL query, validates the schema, and optionally cleans the resulting DataFrame.
    
    Attributes:
    -----------
    conn : psycopg2.extensions.connection
        Connection to the AWS RDS database.
    cursor : psycopg2.extensions.cursor
        Cursor for executing database queries.
    query_package : dict or None
        Package containing query details, source, and join information.
    schema : list or None
        List of expected columns in the DataFrame.
    clean_list : list
        List of cleaning steps to be applied to the DataFrame.
    df : pd.DataFrame
        DataFrame to hold query results.
    removed : pd.DataFrame
        DataFrame to store rows removed during cleaning.
    duplicates : pd.DataFrame
        Dataframe that stores the removed duplicates from the DataFrame.
    cleaning_steps : list
        List to log versions of the DataFrame at each cleaning step.
    archive : list
        Contains the list of files to be added to the S3 Archive
    fields_missing : list
        List of fields missing from the DataFrame if schema validation fails.

    Explanation:
    ------------
    The `RDS` class provides a structured way to handle database interactions with AWS RDS, including querying 
    and cleaning data to meet specific schema requirements. The class can automatically execute a query and 
    process the data upon initialization or wait until explicitly called to generate the DataFrame. It leverages 
    the `psycopg2` library for database connectivity and `pandas` for data manipulation and validation.
    """


    def __init__(self, conn, cursor, query_package = None, auto = True, schema = None, exclude = None, query = None):
        
        #Store Connector and Query Package 
        self.conn = conn
        self.cursor = cursor
        self.query_package = query_package

        #Unpack the Query
        self.source, self.join_list = unpack_query(query_package)

        #Create Schema, Query, and Clean List
        self.schema = build_schema(source = self.source, join_list = self.join_list, schema = schema, exclude = exclude)
        self.query = build_query(query = query, source = self.source, join_list = self.join_list)
     
        #Store Empty Variables for Later Use
        self.duplicates = pd.DataFrame()
        self.archive = []
        self.fields_missing = []

        #If Automatically Generate Data is set to True, Generate Data
        if auto == True:
            self.df = self.query_to_df()

        #If Automatically Generate Data is set to False, Let DF be blank till query is called
        elif auto == False:
            self.df = pd.DataFrame()

        
        


    def check_schema(self, df):

        """
        Validates if the DataFrame matches the expected schema defined in the class.

        Parameters:
        -----------
        df : pd.DataFrame
            The DataFrame to be checked against the schema.

        Returns:
        --------
        bool
            True if the DataFrame matches the schema, False otherwise.

        Raises:
        -------
        Exception
            If the DataFrame is empty.

        Explanation:
        ------------
        This method checks if the DataFrame (`df`) contains all the fields specified in the class attribute `self.schema`. 
        If any fields are missing, it sets the check to False and logs the missing fields in `self.fields_missing`. 
        If the DataFrame is empty, an exception is raised. The function ensures that the DataFrame structure aligns with the expected schema.

        Note:
        -----
        Any discrepancy between the DataFrame and the schema, or an empty DataFrame, will result in an exception.
        """

        #Set Check
        check = True
        
        #Check if DF Empty
        if df.empty == False:
        
            #Check if Fields Exist in DF
            for field in self.schema:
                if field not in df.columns:
                    
                    #Field not Found, Set Check to False and Add Missing Field to List
                    check = False
                    self.fields_missing.append(field)

        else:
            raise Exception("Error: Dataframe Empty When Checking Schema")

        return check
    


    def query_to_df(self):
        
        """
        Executes the stored SQL query, validates the schema, and optionally cleans the resulting DataFrame.

        Parameters:
        -----------
        clean : bool, optional
            If True, performs cleaning operations on the DataFrame. Default is True.

        Returns:
        --------
        pd.DataFrame
            The resulting DataFrame from the SQL query after schema validation and optional cleaning.

        Raises:
        -------
        Exception
            If the SQL query execution fails, the DataFrame schema does not match the expected schema, 
            or if the resulting DataFrame is empty.

        Explanation:
        ------------
        This method executes the stored SQL query (`self.query`) and validates the resulting DataFrame against 
        the expected schema (`self.schema`). It drops duplicate rows and updates the class attribute DataFrame (`self.df`). 
        If the `clean` parameter is set to True, it also applies cleaning operations to the DataFrame. 
        If the DataFrame schema does not match, it raises an exception and lists the missing fields.

        Note:
        -----
        - Ensure that `self.query` and `self.schema` are set before calling this method.
        - Any mismatch in the DataFrame schema or an empty result from the SQL query will raise an exception.
        """
        
        
        if (self.query != None) & (self.schema != None):

            #Update DataFrame with SQL Query
            data = rds_sql_pull(self.cursor, self.query)

            #Check if Data Empty
            if data.empty == False:
                #Check Schema
                if self.check_schema(data):
                    
                    #Drop Duplicate Data
                    data = data.drop_duplicates()

                    #Store Duplicate Data
                    self.duplicates = data[data.duplicated()]
                    
                    #Update DF with Data
                    self.df = data

                    #Create Archive Package
                    self.archive = {
                        "Duplicates": self.duplicates,
                        "Data": self.df
                    }


                    #Return DataFrame
                    return self.df
                

                else:
                    print(f'Missing Fields from Table:  {self.fields_missing}')
                    raise Exception("Error: DataFrame Schema Did Not Match, Check fields_missing()")
                
            else:
                raise Exception("Error: DataFrame Emtpy from SQL Query") 

        else:
            raise Exception("Error: DataFrame Emtpy from SQL Query")