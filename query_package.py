
def set_project(sel_project):
    
    try:

        #Project Dictionary
        project_dictionary = {
            "Helene": 34,
            'Mitlon': 37 
        }

        project_id = project_dictionary[sel_project]
        
        print(f"You've selected  {sel_project}  ID:{project_id}") 
        print("If the name and id are correct please proceed.  If not, please review and update the project dicitonary in query_package.py")

        return project_id


    except Exception as e:
        raise Exception("Project Not Contained in Project Dictionary, Please Review Project Name or Update the Dictionary in query_package.py")
    





def build_query_package(project_val, questions):

    # Create Source Information
    source = {
        #Main for Joining as Application ID as Unique ID
        'table': 'applications_application',

        #Alias Name for Table
        'name' : 'app',

        #Fields to Pull from 
        'fields': [{'application_number' : 'application_number',
                        'created_at' : 'created_at'}],

        #Which Overal Project to Pull Data from, Comes from User Selection
        'project': project_val,

        #Orders by Application ID
        'order': 'id'
    }

    # Create Query Package
    query_package = {'source':source, 'join_list': questions}

    return query_package








# EXAMPLE QUERY
join_list = [
    
    {'name': 'hotel_name',
    'single_or_repeat': 'SINGLE',
    'data_source': 'application_data_textboxanswer',
    'question_id': 1015,
    'fields' : [{'value':'hotel_name'}],
    'clean' : []
    },


    {'name': 'hotel_address',
    'single_or_repeat': 'REPEATING',
    'question_id': 1016,
    'data_source': 'application_data_addressanswer',
    'fields': [{'line1':'hotel_address_line_1',
                'line2': 'hotel_address_line_2',
                'city' : 'hotel_city',
                'state': 'hotel_state',
                'zip' : 'hotel_zip'}],
    'clean' : []
    },


    {'name': 'hotel_status',
    'single_or_repeat': 'REPEATING',
    'data_source': 'application_data_singleselectanswer',
    'question_id': 1013,
    'fields': [{'value':'hotel_status'}],
    'clean' : []
    },


    {'name': 'license_in',
    'single_or_repeat': 'REPEATING',
    'data_source': 'application_data_dateanswer',
    'question_id': 1021,
    'fields': [{'value':'license_in'}],
    'clean' : [{'license_in': ['DATE_CONVERT']}]
    },


    {'name': 'license_out',
    'single_or_repeat': 'REPEATING',
    'data_source': 'application_data_dateanswer',
    'question_id': 1022,
    'fields': [{'value':'license_out'}],
    'clean' : [{'license_out': ['DATE_CONVERT']}]
    },


    {'name': 'total_in_household',
    'single_or_repeat': 'SINGLE',
    'data_source': 'application_data_numberanswer',
    'question_id': 596,
    'fields': [{'value':'total_in_household'}],
    'clean' : []
    },


    {'name': 'active_bookings',
    'single_or_repeat': 'SINGLE',
    'data_source': 'application_data_numberanswer',
    'question_id': 1010,
    'fields': [{'value':'active_bookings'}],
    'clean' : []
    },

    {'name': 'pathway_determination',
    'single_or_repeat': 'SINGLE',
    'data_source': 'application_data_singleselectanswer',
    'question_id': 632,
    'fields': [{'value':'pathway_determination'}],
    'clean' : []
    }
]