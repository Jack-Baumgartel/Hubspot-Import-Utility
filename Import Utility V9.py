#add a welcome message
print("\n\nWelcome to Jack Baumgartel's HubSpot import utility. This tool enables the bulk import of contacts as \
provided in a .csv or .xls file from the VAR Resource Center. Please stand by as necessary dependencies are loaded ... ")


#define easy to use parameters for testing & editing
testing = False #can be True or False, true if in development, False when launched
task_assignee = 'Name 1'
review_assignee = 'Name 2'
contact_owner = 'Name 3'

#prevent immediate close on unexpected error
try:
    #import external dependencies
    import pandas as pd
    from hubspot.auth.oauth import ApiException
    from hubspot import HubSpot
    from hubspot.crm.contacts import SimplePublicObjectInput
    from hubspot.crm.contacts.exceptions import ApiException
    import time
    import easygui
    import math
    import requests
    import json
    import numpy as np
    import os


    def pull_existing_contacts():
        '''Help: With an active "api_client", pull all contacts currently in HubSpot and return a Pandas dataframe
        containing just their names & email addresses. Also prints a status message on completion.'''
        global hscontacts, all_contacts
        #first use the api_client to get all HubSpot contacts
        all_contacts = api_client.crm.contacts.get_all()

        #create a new blank dataframe to store these hubspot contacts
        hscontacts = pd.DataFrame(columns=['firstname', 'lastname', 'email'])

        #iterate over the hubspot generated contact list, and add them each to the dataframe
        #done manually because Pandas built in functions were not giving the desired result
        for count in range(len(all_contacts)):
            #get the info of the current item
            hscontact = all_contacts[count].to_dict()

            #pull necessary info
            email = hscontact['properties']['email']
            firstname = hscontact['properties']['firstname']
            lastname = hscontact['properties']['lastname']

            #compile the data and append it to the dataframe
            data_to_add = [firstname, lastname, email]
            hscontacts.loc[len(hscontacts)] = data_to_add

        return hscontacts


    if testing:
        #establish test authentication using our access & bearer tokens
        api_client = HubSpot(access_token='***********')  #access token for test account #1
        bearer_token = '**************'
        owner_ids = {'Name': 1234567}
        
        task_assignee = 'Name 1'
        review_assignee = 'Name 2'
        contact_owner = 'Name 3'


    elif not testing:
        #establish real authentication using our access & bearer tokens
        api_client = HubSpot(access_token='**************')  #access token for real account
        bearer_token = '****************'
        #dictionary defining the owner_ids from hubspot
        owner_ids = {'Name 1': 1234567,
                     'Name 2': 1234567,
                     'Name 3': 1234567}






    #give update to user
    print("\nPlease select the .csv or .xls file containing the new contacts to be imported ...\n")
    time.sleep(1)


    #ask user where the file to import is stored
    raw_data_filename = easygui.fileopenbox()

    if raw_data_filename.endswith('csv'):
            csv=True
            #load the VRC exported file as a pandas DF
            raw_data = pd.read_csv(raw_data_filename, encoding='utf_16_le', sep='\t', index_col=False)
    elif raw_data_filename.endswith('xls'):
            csv=False
            raw_data = pd.read_excel(raw_data_filename, index_col=False)
    else:
        print('Selected file was not a .csv or .xls, please select an appropriate file.')
        time.sleep(10)
        exit()

    #set of contacts to exlcude all partners
    non_partners = raw_data[raw_data['AccountType'] != 'Partner'].reset_index().drop(columns=['index'])

    #create a new, blank df for the entries we want to add
    df_to_import = pd.DataFrame(columns=['firstname', 'lastname', 'company', 'product', 'request', 'zipcode', 
                                         'phone', 'email', 'leadrating', 'contactsource', 'lifecyclestage', 
                                         'leadstatus', 'vrcsource', 'vrcleadid', 'importsource', 'importstatus',
                                         'accounttype'])

    #define the source variable for all contacts imported this way
    contactsource = 'Source Name'
    lifecyclestage = 'lead'
    leadstatus = 'NEW'
    importsource = time.strftime("VRC Bulk Import Tool @ %I-%M %p %B %d, %Y", time.localtime())
    importstatus = 'Unsorted'

    #set the dataframe to use
    df = non_partners

    #define request strings in the "Source" column to look for
    request_options = ['RAC', 'RAD', 'RAQ']

    #iterate through the provided dataframe
    for index in range(len(df)):
        #pull necessary data for customer
        current_data = df.loc[index]
        firstname = current_data['FirstName']
        lastname = current_data['LastName']
        company = current_data['CompanyName']
        product = current_data['Product']
        #see if any contact was requested, and note which has been
        requested = [request_item for request_item in request_options if request_item in current_data['Source']]
        zipcode = str(current_data['Postal Code'])
        accounttype = current_data['AccountType']

        #add 'None' to the requested list if they did not request
        if not bool(requested):
            requested.append('None')

        #only add a phone number if they can be called
        if current_data['Contact Never Call'] != 'Y':
            if not csv:
                try:
                    phone = int(str(current_data['ContactPhone']))
                except:
                    phone = None
            else:
                try:
                    phone = str(current_data['ContactPhone']).replace('(', '').replace(')', '\
                    ').replace(' ','').replace('+','').replace('-','')
                    phone = int(phone)
                    never_call = False
                except:
                    phone = None
        else:
                phone = None # or '0000000000'
                never_call = True

        #check if they have provided an email, and can be emailed, if not set a dummy value
        if '@' in current_data['EmailAddress'] and current_data['Contact Never Email'] != 'Y':
            email = current_data['EmailAddress']
            never_email = False
        else:
                email = None  # or fakeemail@notreal.com
                never_email = True

        #add the lead rating from the VRC
        if isinstance(current_data['Lead Rating'], str):
            if '1' in current_data['Lead Rating']:
                lead_rating = 'Hot'
            elif '5' in current_data['Lead Rating']:
                lead_rating = 'Cold'     
        else:
            lead_rating = None

        #pull final raw data from VRC
        vrcsource = current_data['Source']
        if csv:
            vrcleadid = current_data['Lead Id']
        else:
            vrcleadid = current_data['LeadId']


        #format the data to be added to the new dataframe
        info_to_add = [firstname, lastname, company, product, requested, zipcode, 
                       phone, email, lead_rating, contactsource, lifecyclestage, leadstatus, 
                       vrcsource, vrcleadid, importsource, importstatus, accounttype]

        #add the contact as a new row to the dataframe
        df_to_import.loc[len(df_to_import)] = info_to_add


    #convert phone column to strings, then int dtypes for hubspot compatibility
    str_df = df_to_import.astype({"phone": str})
    #int_df = str_df.astype({"phone": int})

    print(f"Successfully loaded {len(df_to_import)} new contacts from {raw_data_filename}\n")





    #Import / sequence logic

    df = str_df
    print(f'Now sorting & categorizing contacts... \n')

    #list all possible product values from VRC
    all_products = set(df['product'].to_list())

    #sort items into buckets based on product
    bucket_1_products = ['3DEXPERIENCE WORKS', 'SolidWorks', 'SOLIDWORKS Cloud Offer',
                         '3D Creator', '3D Sculptor', '3DXWorks Management',
                         'Visualize', '3DXWorks Manufacturing', '3DXWorks SOLIDWORKS',
                         'Multi-Product']
    bucket_2_products = ['Simulation', 'PDM', 'Electrical', 'Plastics', 'Flow Simulation',
                         'CAM', 'Composer', '3DXWorks Simulation']
    bucket_3_products = ['DraftSight', '3DEXPERIENCE DraftSight']
    #put all unused products into a fourth bucket
    bucket_4_products = [x for x in all_products if x not in bucket_1_products + bucket_2_products + bucket_3_products]

    #create a blank list of contact rows to be deleted
    rows_to_drop = []

    #sift through contact list again and filter based on custom sorting criteria
    for index in range(len(df)):
        #pull necessary data for customer
        current_data = df.loc[index]
        name = f"{current_data['firstname']} {current_data['lastname']}"

        #skip any contacts which lack both an email & phone
        if current_data['phone'] != 'None' or current_data['email']:
            print(f"{name} ", end='')

            #first check if their lead status is cold, and they requested nothing skip all further steps if so
            if current_data['leadrating'] != 'Cold' or 'None' not in current_data['request']:

                #now see if the lead's product falls into the first bucket
                if current_data['product'] in bucket_1_products:
                    #if they request anything, or are a HOT lead, mark for SW Hot Lead Sequence
                    if current_data['request'] != ['None'] or current_data['leadrating'] == 'Hot':
                        print("marked for SOLIDWORKS Hot Lead Sequence.")
                        df.at[index, 'importstatus'] = 'SOLIDWORKS Hot Lead Sequence'

                    #otherwise mark for SW lead Sequence
                    else:
                        print("marked for SOLIDWORKS Lead Sequence.")
                        df.at[index, 'importstatus'] = 'SOLIDWORKS Lead Sequence'


                #now check if their prodcut is in bucket 2
                elif current_data['product'] in bucket_2_products:
                    #if they requested anything, or are marked as a hot lead, add task for stephen
                    if current_data['request'] != ['None'] or current_data['leadrating'] == 'Hot':
                        print("marked for Hot Lead Sequence.")
                        df.at[index, 'importstatus'] = 'Hot Lead Sequence'

                        #exclude others from import
                    else:
                        print("lacks bucket 2 criteria, note only.")
                        df.at[index, 'importstatus'] = 'Bucket 2 - note only'
                        df.at[index, 'internalstatus'] = 'Bucket 2 - note only'

                #now check if their prodcut is in bucket 3
                elif current_data['product'] in bucket_3_products:
                    #if they requested anything, or are marked as a hot lead, mark for draftsight sequence
                    if current_data['request'] != ['None'] or current_data['leadrating'] == 'Hot':
                        print("marked for Draftsight Lead Sequence.")
                        df.at[index, 'importstatus'] = 'Draftsight Lead Sequence'

                    #exclude others from import
                    else:
                        print("lacks bucket 3 criteria, note only.")
                        df.at[index, 'importstatus'] = 'Bucket 3 - note only'
                        df.at[index, 'internalstatus'] = 'Bucket 3 - note only'

                #finally clean up any stragglers in bucket 4
                elif current_data['product'] in bucket_4_products:
                    print("product in bucket 4, note only.")
                    df.at[index, 'importstatus'] = 'Bucket 4 - note only'
                    df.at[index, 'internalstatus'] = 'Bucket 4 - note only'

            else:
                print("is cold, nothing requested.")
                df.at[index, 'importstatus'] = 'Cold lead - note only'
                df.at[index, 'internalstatus'] = 'Cold lead - note only'

        else:
            #add the contact into the list of rows to be deleted
            rows_to_drop.append(index)
            
        if current_data['vrcsource'] == 'eDrawings Activation':
            df.at[index, 'importstatus'] = 'eDrawings Activation Sequence'
    
    #delete marked rows and recount index
    sorted_df = df.drop(rows_to_drop).reset_index(drop=True)





    print("\n\nImporting contacts to HubSpot now ... \n")
    time.sleep(1)

    df = sorted_df

    #record the start time for the program
    start_time = time.time()

    #iterate through and add all contacts
    for index in range(len(df)):
        #pull data for current entry
        contact = df.loc[index]
        name = f"{contact['firstname']} {contact['lastname']}"

        #reset the contact_id
        contact_id = 0

        #prepare the contact object
        simple_public_object_input = SimplePublicObjectInput(
            properties={"email": contact['email'],
                        "firstname": contact['firstname'],
                        "lastname": contact['lastname'],
                        "phone": contact['phone'],
                        "company": contact['company'],
                        "contact_source": contact['contactsource'],
                        "lifecyclestage": contact['lifecyclestage'],
                        "hs_lead_status": contact['leadstatus'],
                        "zip": str(contact['zipcode']),
                        "requested": contact['request'][0], 
                        "vrc_source": contact['vrcsource'],
                        "vrc_lead_id": contact['vrcleadid'],
                        "input_source": contact['importsource'],
                        "import_sequence": contact['importstatus']
            })

        try:
            print(f"\n Attempting to import {name} ...", end='')
            api_response = api_client.crm.contacts.basic_api.create(
                simple_public_object_input=simple_public_object_input)
            #if succeeded, say so and proceed accordingly

            print(f" success, imported to HubSpot.", end='')
            df.at[index, 'internalstatus'] = 'Successfully imported.'

            #add the contact id
            contact_id = api_response.id
            df.at[index, 'contact_id'] = contact_id

        except Exception as e:
            msg = e
            #if failed because contact already exists
            if "Contact already exists." in str(e):
                print(' failed, already exists.', end='')
                contact_id = int(str(e).split('Existing ID:')[1].split('"')[0])
                df.at[index, 'internalstatus'] = 'Already in HubSpot.'

                #add the contact id
                df.at[index, 'contact_id'] = contact_id

            #otherwise just note the error and move on
            else:
                error_msg = str(e)
                print(f"failed: {error_msg}.", end='')
                df.at[index, 'internalstatus'] = f'Failed with API exception:{error_msg}'


        #adjust the contacts input_source property, regardless of import status
        simple_public_object_input = SimplePublicObjectInput(properties={"input_source": contact['importsource']})

        try:
            #edit the input_source property for existing contacts
            api_response = api_client.crm.contacts.basic_api.update(contact_id = contact_id,
                simple_public_object_input=simple_public_object_input)
        except:
            print('Error adjusting input_source!')

        #try to add a note and associate it to the contact, regardless of import status
        try:

            #format a human readable note
            note_body = f"{name} is interested in {contact['product']} and requested {contact['request'][0]}. They \
    work for {contact['company']} with phone number {contact['phone']}. Their VRC 'source' is {contact['vrcsource']} and \
    their VRC lead ID is {contact['vrcleadid']}."

            #define properties of the note
            properties = {'hs_timestamp': f'{int(time.time())*1000}', 
                    'hs_note_body': note_body,
                    'hubspot_owner_id': str(owner_ids[contact_owner])}

            #create a note object
            simple_public_object_input = SimplePublicObjectInput(properties=properties)

            #submit the note to hubspot
            note_response = api_client.crm.objects.notes.basic_api.create(
                simple_public_object_input=simple_public_object_input)

            #and associate it with the current contact
            note_id = note_response.id
            put_url=f'https://api.hubapi.com/crm/v3/objects/notes/{note_id}/associations/contact/{contact_id}/note_to_contact'

            note_put = requests.put(put_url, headers = {'Authorization': bearer_token})
            print(f'  Note {note_id} added successfully.', end='')

            #update the internal status
            status = df.at[index, 'internalstatus']
            df.at[index, 'internalstatus'] = f"{status} Note {note_id} added."


        except ApiException as e:
            error_msg = str(e)
            print(f"  Failed to add note: {error_msg}.")

            status = df.at[index, 'internalstatus']
            df.at[index, 'internalstatus'] = f"{status} Note addition failed."

        #add a task for follow up if contact is a commercial contact
        if contact['accounttype'] == 'Commercial':
            #define properties of the task using contact's information
            properties = {'hs_timestamp': f'{int(time.time()+60*60*24*2)*1000}', #add a due date two days from now
                    'hs_task_body': f"{name} was marked for follow up. They are interested in \
            {contact['product']} and requested contact: {contact['request'][0]}",
                    'hs_task_subject': f'Review VRC Lead: {name}',
                    'hubspot_owner_id': str(owner_ids[task_assignee]),
                    'hs_task_type': 'TODO',
                    'hs_task_priority': 'MEDIUM'}

            #create a task object
            simple_public_object_input = SimplePublicObjectInput(properties=properties)

            #submit the task to hubspot
            try:
                task_response = api_client.crm.objects.tasks.basic_api.create(simple_public_object_input=simple_public_object_input)
                #and associate it with the current contact
                task_id = task_response.id
                put_url=f'https://api.hubapi.com/crm/v3/objects/tasks/{task_id}/associations/contact/{contact_id}/task_to_contact'

                note_put = requests.put(put_url, headers = {'Authorization': bearer_token})
                print(f' Task added for {task_assignee}.', end='')

                status = df.at[index, 'internalstatus']
                df.at[index, 'internalstatus'] = f"{status} Task added."

            except ApiException as e:
                error_msg = str(e)
                print(f"  Failed to add Commercial lead task: {error_msg}")

                status = df.at[index, 'internalstatus']
                df.at[index, 'internalstatus'] = f"{status} Task addition failed."
                
        #add a task for review if customer is not in a sequence
        if 'Sequence' not in contact['importstatus']:
            #define properties of the task using contact's information
            properties = {'hs_timestamp': f'{int(time.time()+60*60*24*2)*1000}', #add a due date two days from now
                    'hs_task_body': f"{name} was marked for review. They are interested in \
            {contact['product']} and requested contact: {contact['request'][0]}",
                    'hs_task_subject': f'Review VRC Lead: {name}',
                    'hubspot_owner_id': str(owner_ids[review_assignee]),
                    'hs_task_type': 'TODO',
                    'hs_task_priority': 'MEDIUM'}

            #create a task object
            simple_public_object_input = SimplePublicObjectInput(properties=properties)

            #submit the task to hubspot
            try:
                task_response = api_client.crm.objects.tasks.basic_api.create(simple_public_object_input=simple_public_object_input)
                #and associate it with the current contact
                task_id = task_response.id
                put_url=f'https://api.hubapi.com/crm/v3/objects/tasks/{task_id}/associations/contact/{contact_id}/task_to_contact'

                note_put = requests.put(put_url, headers = {'Authorization': bearer_token})
                print(f' Task added for {review_assignee}.', end='')

                status = df.at[index, 'internalstatus']
                df.at[index, 'internalstatus'] = f"{status} Task added."

            except ApiException as e:
                error_msg = str(e)
                print(f"  Failed to add Commercial lead task: {error_msg}")

                status = df.at[index, 'internalstatus']
                df.at[index, 'internalstatus'] = f"{status} Task addition failed."

        #add a basic time delay to comply with HubSpot rate limits
        time.sleep(.1) 

    #record the end time and format the time change
    end_time = time.time()
    time_elapsed = end_time-start_time
    min_elapsed = math.floor(time_elapsed/60)
    sec_elapsed = time_elapsed - (min_elapsed*60)
    elapsed_str = f"{min_elapsed} min, {round(sec_elapsed,2)} s"


    #export the results as an excel file for future reference
    save_path = f'C:\\Users\\{os.getlogin()}\\Downloads\\'
    results_filename = time.strftime("VRC Lead Import Result (%a, %B %d, %I-%M %p).xlsx", time.localtime())

    df.to_excel(f"{save_path}{results_filename}")

    #and provide a final summary message
    print(f'\n\n\n\
    Time elapsed: {elapsed_str} \n\
    See the complete results of this import in Downloads/{results_filename}\n')

except Exception as unknown_error:
    print(f"\n\n Unhandled error encountered: {unknown_error}")

time.sleep(5)

input('\nPress Enter or X out of the window to close the program. \n\n\n')

