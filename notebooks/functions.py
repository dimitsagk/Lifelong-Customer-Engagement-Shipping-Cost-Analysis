import pandas as pd
from thefuzz import fuzz


def cleaning(df):
    '''Function for data cleaning.
    Receives the dataframe as a parameter, and returns the updated dataframe.'''

    
    # List of columns to be dropped that are not useful for the analysis.
    columns_to_drop = ['Customer phone', 'Currency', 'Payment status',
        'Quantity of items', 'Billing address province/state', 'Shipping method',
        'Ship to the billing address', 'Ship to', 'Ship to company',
        'Shipping address', 'Shipping address 2', 'Shipping address city',
        'Shipping address province/state', 'Shipping address postal code',
        'Shipping address country', 'Token', 'Order discounts',
        'Payment Method', 'Payment Gateway Used', 'Metadata',
        'PaymentGatewayTransactionId', 'Taxes', '21% VAT (incl.)',
        '9% VAT (incl.)', 'email', 'vatnumber', 'Item ID', 'Item name',
        'Item description', 'Item url', 'Unit price', 'Quantity', 'Total price',
        'Total Weight', 'roast', 'weight', 'size', 'color', 'Size', 'ww_kind', 'Company name']
    # Dropping columns that are in the list of columns to drop
    df.drop(columns_to_drop, axis = 1, inplace=True)

    # I am dropping duplicated values in the Invoice number column:
    # because I know the reason for he duplicated values: dataset was stacked
    df.drop_duplicates(subset = ["Invoice number"], inplace=True)

    # Dropping orders that were cancelled
    df = df[df["Order status"] != "Cancelled"]
    
    return df



def formatting(df):
    '''Function that formats dataframe, like datetime, column names etc.   
    Receives the dataframe as a parameter, and returns the updated dataframe.'''    

    
    # Making date column to_datetime
    df["Order date"] = pd.to_datetime(df["Order date"], format = "%Y-%m-%d %H:%M:%S")
    
    # modifications to column names for clarity and brievity
    df.rename(columns= {"Billing address":"Customer address", "Billing address 2":"Customer address 2",
                       "Billing address city":"Customer city",
                       "Billing address postal code":"Customer postal code", 
                        "Billing address country":"Customer country"
                       }, inplace=True)

    #Cleaning some common non valid values from postcode 
    df["Customer postal code"] = df["Customer postal code"].fillna("00000")
    df["Customer postal code"] = df["Customer postal code"].replace(["000000","0","0000","/"],"00000")                                              

    
    # Strips leading and trailing whitespace and converts values in specified string columns to title case
    string_columns = ["Customer name", "Customer address", "Customer address 2", 
                      "Customer city", "Customer postal code"]
    for col in string_columns:
        df[col] = df[col].apply(lambda x: str(x).strip().title())

    # Converts "Customer country" to uppercase and strips whitespace, 
    # "Customer email" to lowercase and strips whitespace
    df["Customer country"] = df["Customer country"].apply(lambda x: str(x).strip().upper())
    df["Customer email"] = df["Customer email"].apply(lambda x: str(x).strip().lower())

    
    # Combines address columns, removes 'Nan', strips whitespace, replaces commas, and drops redundant column
    df["Customer address 2"] = df["Customer address 2"].str.replace('Nan','')
    df["Customer address"] = df["Customer address"] + df["Customer address 2"].apply(lambda x: " " + str(x) if str(x) != '' else '')

    # Getting rid of "," cause I will use it later to split the address
    df["Customer address"] = df["Customer address"].str.replace(","," ")

    # Dropping the column, not useful anymore cause I combined it with "Customer address"
    df.drop("Customer address 2", axis=1, inplace=True)
   
    return df



def new_columns(df):
    '''Function that creates new columns that will be helpful for the analysis.
    New columns are "Customer full address" and "Subtotal products".   
    Receives the dataframe as a parameter, and returns the updated dataframe.'''  

    
    # Making new column with full address
    address_columns_list = ["Customer address", "Customer city",
                        "Customer postal code","Customer country"]
    
    # making sure that there is no existing "," to be able to seperate them after
    df["Customer city"] = df["Customer city"].str.replace(",", " ")
    df["Customer postal code"] = df["Customer postal code"].str.replace(",", " ")
    df["Customer country"] = df["Customer country"].str.replace(",", " ")

    # Joining them with ","
    df["Customer full address"] = df[address_columns_list].apply(lambda x: ','.join(x), axis=1)

    
    # Creates column with only the total cost of the products, before any taxes/ shipping costs/ discounts etc 
    # Also deletes the columns that are not needed after the calculation
    df["Subtotal products"] = df["Grand total"] - df["Shipping fees"] + df["Discounts total"] - df["Refunds amount"] - df["Taxes total"]
    
    # In this project I will only work with the subtotal products amount, so I am deleting the other columns
    columns_to_drop = ["Sub total","Grand total","Adjusted total","Refunds amount","Discounts total","Taxes total"]
    df.drop(columns_to_drop, axis = 1, inplace=True)
   
    return df



def fuzzy_match_clients(df):
    '''Identifies and corrects similar client information, often resulting from manual entry of addresses by clients.
    This function uses fuzzy matching with a ratio between >85 and <100 to identify similar client records. The chosen ratio balances 
    accurately identifying the same clients while allowing for some separation, particularly for clients with multiple stores. 
    Priority is given to preserving distinctions between stores for analysis purposes.
    Receives the dataframe as a parameter, and returns the updated dataframe.'''  
    
    
    df["test"] = df["Customer name"] + "-- "+ df["Customer email"] + "-- "+ df["Customer full address"]
    
    test_list = list(set(df["test"]))
    
    test_list = list(set(df["test"]))
    info_correct = []

    for i, element in enumerate(test_list):
        addition = 1
        while i + addition < len(test_list):
            ratio_fuzz = fuzz.ratio(test_list[i], test_list[i+addition])
        
            if 85 < ratio_fuzz < 100:
                if (test_list[i+addition], test_list[i]) not in info_correct and (test_list[i], test_list[i+addition]) not in info_correct:
                    info_correct.append((test_list[i+addition], test_list[i]))
                elif (test_list[i+addition], test_list[i]) not in info_correct and (test_list[i], test_list[i+addition]) in info_correct:
                    # Find the tuple with original value test_list[i] and append the new value test_list[i+addition]
                    info_correct[info_correct.index((test_list[i], test_list[i+addition]))].append(test_list[i+addition])
                elif (test_list[i], test_list[i+addition]) not in info_correct:
                    # Find the tuple with original value test_list[i+addition] and append the new value test_list[i]
                    info_correct[info_correct.index((test_list[i+addition], test_list[i]))].append(test_list[i])

            addition += 1

    # Replace values in the 'test' column of the DataFrame using list comprehension and map
    df["test"] = df["test"].apply(lambda x: next((new_value for original_value, new_value in info_correct if original_value == x), x))
    
    
    df[["Customer name", "Customer email", "Customer full address"]] = df["test"].str.split("--", expand=True)
    df[["Customer address", "Customer city","Customer postal code","Customer country"]] = df["Customer full address"].str.split(",", expand=True)
    
    # deleting the "test" column cause I no longer need it
    df.drop("test", axis = 1, inplace=True)
   
    return df



def anonymize_clients(df):
    '''Creates a column with a code for each unique client'''
    
    # first making a column with all the client info combines, to be easier to identify unique customers
    df["Unique clients"] = df["Customer name"] + ","+ df["Customer email"] + ","+ df["Customer full address"]
    
    unique_clients = df["Unique clients"].unique()
    
    # adding ::-1 to start from the bottom of the dataframe and move up
    client_codes = {client: f'Client_{i}' for i, client in enumerate(unique_clients[::-1])}
    
    df['Client codes'] = df['Unique clients'].map(client_codes)
  
    return df



def dropping_column_with_private_info(df):
    ''' Deleting the columns that contain private info of clients'''
    
    # Optional, to export the csv with the client info as it is, if needed
    df.to_csv("csv with client info")
    
    # Deleting columns that contain private info from the clients
    columns_to_drop = ['Customer name',"Customer email","Customer address","Customer full address","Unique clients"]
    df.drop(columns_to_drop, axis = 1, inplace=True)
    
    return df