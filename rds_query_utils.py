"""
This Python script encompasses utility functions specifically designed to process query results from the 
Tidal Basin PostgreSQL database hosted on AWS RDS. These functions are integral components of the rds_processor.py file.


Functions:

    - rds_sql_pull(cursor, query): 
      Executes a SQL query using the provided cursor, fetches rows, and converts them to a Pandas DataFrame.
      
    - build_schema(source=None, join_list=None, schema=None, exclude=None): 
      Constructs a schema list from the source and join list, optionally excluding specified fields. 
      
    - build_query(source=None, join_list=None, query=None): 
      Constructs a SQL query from the source and join list. 


      
Dependencies:
    - pandas
    - codecs
    - datetime
    - rds_clean_utils

Created by: Charles Ross
Contact: charles.ross@mbakerintl.com
Last updated by: Charles Ross on 12/03/2024
"""




################    IMPORT PACKAGES    ######################
import pandas as pd
import codecs



################    RDS QUERY UTILITY FUNCTIONS    ######################


#----------------------------------------------------------------

def rds_sql_pull(cursor, query):

    """
    Executes a SQL query using the provided cursor, fetches rows, and converts them to a Pandas DataFrame.

    Parameters:
    - cursor: Cursor object for database connection.
    - query: SQL query string to be executed.

    Explanation:
    This function attempts to execute a SQL query using the given cursor. It fetches all rows from the query result 
    and then converts these rows into a Pandas DataFrame. If the conversion to a DataFrame fails, an exception is raised, 
    and the process is stopped to indicate the failure. The function ensures that the SQL query is executed 
    and its results are correctly transformed into a DataFrame format for further data manipulation or analysis.

    Return:
    The DataFrame containing the query results if successful.

    Note:
    Any failure in executing the query or converting the result to a DataFrame triggers an exception, 
    alerting the user to address the issue promptly.
    """

    try:

        # Execute Query
        cursor.execute(query)
        # Fetch Columns
        columns = [col[0] for col in cursor.description]
        # Fetch Rows
        rows = cursor.fetchall()


        try:
        
            #Convert to Dataframe
            df = pd.DataFrame(rows, columns=columns)
            return df


        except Exception as e:
            
            # Raise Exception to Stop Process if Failure
            raise Exception(f"Rows Pulled from Cursor, Failed to Convert to Pandas Dataframe: {e}")


    except Exception as e:

        # Raise Exception to Stop Process if Failure
        raise Exception(f"Failed to Pull Rows from Cursor Query Execute: {e}")
    




#----------------------------------------------------------------

def unpack_query(query_package):

    """
    Unpacks the query package to extract the source and join list components.

    Parameters:
    -----------
    query_package : dict
        A dictionary containing the query details including source and join list components.

    Returns:
    --------
    tuple
        A tuple containing the source component and the join list component.

    Raises:
    -------
    Exception
        If the query package exists but does not contain the source or join list components.

    Explanation:
    ------------
    This function checks if the `query_package` is not None and then attempts to extract the 'source' and 'join_list' components from it. 
    If either component is missing, an exception is raised indicating the specific missing component. If the `query_package` is None, 
    both the source and join list are set to None. The function ensures that the necessary components are present for further processing 
    and returns them as a tuple.

    Note:
    -----
    Any discrepancy in the `query_package`, such as missing source or join list, will raise an exception.
    """

    #If Join List NOT Empty
    if query_package != None:
        
        if 'source' in query_package:
            source = query_package["source"]
        else:
            source = None
            raise Exception("Query Package Exists, but Not Source Component Identified")
        
        if 'join_list' in query_package:
            join_list = query_package["join_list"]
        else:
            source = None
            raise Exception("Query Package Exists, but Not Join List Component Identified")    
                           
    #Join List Empty           
    else:
        source = None
        join_list = None     


    #Return Clean List
    return source, join_list





#----------------------------------------------------------------

def build_schema(source = None, join_list = None, schema = None, exclude = None):
    
    """
    Constructs a schema list from the source and join list, optionally excluding specified fields.

    Parameters:
    -----------
    source : dict, optional
        A dictionary containing the source table and its fields.
    join_list : list, optional
        A list of dictionaries containing the join tables and their fields.
    schema : list, optional
        A list of expected columns in the DataFrame.
    exclude : list, optional
        A list of fields to be excluded from the schema.

    Returns:
    --------
    list
        A list of fields representing the constructed schema.

    Raises:
    -------
    Exception
        If there is an error building the schema from the source or join list.

    Explanation:
    ------------
    This function creates an empty schema list and populates it with fields from the source and join list if provided. 
    If the schema is manually passed into the function, it uses that schema. It also filters out any fields specified 
    in the exclude list. If any error occurs during the construction of the schema from the source or join list, 
    an exception is raised. The function ensures that the resulting schema list contains the required fields for further processing.

    Note:
    -----
    Any discrepancy in the source or join list data packages will raise an exception.
    """

    # Create Empty Schema List
    schema_lst = []

    #If Source and Join List Data Present
    if (schema == None) & (source != None) & (join_list != None):
        
        try:
            #Add Fields from Source
            for field in source['fields']:
                for tn, jn in field.items():
                    schema_lst.append(jn)


            # Add Fields from Joins
            for item in join_list:
                for field in item['fields']:
                    for tn, jn in field.items():
                            schema_lst.append(jn)
    
        except Exception as e:
            raise Exception(f"Error: Could not build schema from source or join list, check data packages.  Traceback: {e}")


    #If Source Manually Passed Into Function
    elif (schema != None) & (source == None) & (join_list == None):
        schema_lst = schema

    #Filter Schema List
    if exclude != None:
        schema_lst = [field for field in schema_lst if field not in exclude]

    #Return Schema List
    return schema_lst


#----------------------------------------------------------------

def build_query(source = None, join_list = None, query = None):

    """
    Constructs a SQL query from the source and join list or uses a provided query.

    Parameters:
    -----------
    source : dict, optional
        A dictionary containing the source table and its fields.
    join_list : list, optional
        A list of dictionaries containing the join tables and their fields.
    query : str, optional
        An existing SQL query string to be used.

    Returns:
    --------
    str
        The constructed SQL query string.

    Raises:
    -------
    Exception
        If there is an error pulling fields from the source or join list data packages, or applying project filters and order operations.

    Explanation:
    ------------
    This function constructs a SQL query by first initializing the query string with a SELECT statement. It adds fields from 
    the source table and join tables, includes JOIN clauses for the join tables, and applies a WHERE clause to filter by project 
    and an ORDER BY clause to sort the results. If the source and join list are not provided, it returns the existing query 
    passed as an argument. If there is any error during the construction of the query, an exception is raised. The function 
    ensures that the resulting query string is correctly formatted and ready for execution.

    Note:
    -----
    Any discrepancy in the source or join list data packages, or failure to apply filters and order operations, will raise an exception.
    """
    
    #If Source and Join List Passed, Query 
    if (query == None) & (source != None) & (join_list != None):
        
        #Set Count for Join Sections
        join_count = 1

        #Initialize Query Select
        query = "SELECT\n"

        #Add Fields from Source
        try:
            for field in source['fields']:
                for tn, jn in field.items():
                    query += f"    {source['name']}.{tn} AS {jn},\n"

        except Exception as e:
            raise Exception(f"Error: Could not pull fields from Source, check Source data package.  Traceback: {e}")



        # Add Fields from Joins
        try:
            for index, item in enumerate(join_list):
                # Add Join Segments
                for field in item['fields']:
                    for tn, jn in field.items():
                        if index == len(join_list) - 1 and field == item['fields'][-1]:
                            query += f"    {item['name']}.{tn} AS {jn}\n"
                        else:
                            query += f"    {item['name']}.{tn} AS {jn},\n"

        except Exception as e:
            raise Exception(f"Error: Could not pull fields from Join List, check Join List data package.  Traceback: {e}")



        #Add Source
        try:
            query += "\nFROM\n"
            query += f"    {source['table']} {source['name']}\n"
        
        except Exception as e:
            raise Exception(f"Error: Could not pull source table or name from Source data package.  Traceback: {e}")



        #Add Joins
        try:
            for index, item in enumerate(join_list):

                #Set Version of Join Through Table
                if index == 0:
                    data_conn = 'initial_join_answers'
                else:
                    data_conn = f'join_answers_{join_count}'
                    join_count += 1

                
                #Identify if Question is Single or Repeating
                if item['single_or_repeat'] == 'SINGLE':
                    
                    query += f"\nLEFT JOIN application_data_answer {data_conn} ON app.id = {data_conn}.application_id AND {data_conn}.question_id = {item['question_id']}"
                    query += f"\nLEFT JOIN {item['data_source']} {item['name']} ON {data_conn}.id = {item['name']}.answer_ptr_id\n"


                elif item['single_or_repeat'] == 'REPEATING':
                    
                    query += f"\nLEFT JOIN application_data_answer {data_conn} ON initial_join_answers.repeating_answer_section_id = {data_conn}.repeating_answer_section_id AND {data_conn}.question_id = {item['question_id']}"
                    query += f"\nLEFT JOIN {item['data_source']} {item['name']} ON {data_conn}.id = {item['name']}.answer_ptr_id\n"
                
                
        except Exception as e:
            raise Exception(f"Error: Could not pull join information from join list, check join list data package.  Traceback: {e}")


        #Filter Project
        try:
            query += "\n WHERE \n"
            query += f"    {source['name']}.project_id = {source['project']}"

        except:
            raise Exception(f"Error: Could not apply project filter to query.  Traceback: {e}")


        #Order Project
        try:
            query += "\n ORDER BY\n"
            query += f"{source['name']}.{source['order']};"
        
        except:
            raise Exception(f"Error: Could not apply order operations to query.  Traceback: {e}")
        
            
        #Encode String
        query = codecs.decode(query.encode(), 'unicode_escape')



    # If Query Passed, Pass Back Query
    elif (query != None) & (source == None) & (join_list == None):
        query = query


    #Return Query
    return query