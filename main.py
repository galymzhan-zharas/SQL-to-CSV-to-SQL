# database name : customers 

# TASK 0: clean the data
    # 3 csv files contain garbage values (extra symbols, etc)
    # columns are not consistent: they are located differently 
# TASK 1: merge 3 customers table into 1
    # https://www.freecodecamp.org/news/how-to-combine-multiple-csv-files-with-8-lines-of-code-265183e0854/
    # https://towardsdatascience.com/how-to-merge-large-csv-files-into-a-single-file-with-python-c66696f595ff
    # https://www.delftstack.com/howto/python/merge-csv-files-python/
# TASK 2: turn csv to sql
    # import csv_to_sql from my_ds_babel 
# IMPORTANT: remove csv files after merging
    # Google: gitignore file
    
import pandas as pd
import re

def clean_up(csv_file_name_1, csv_file_name_2, csv_file_name_3):
    # 1. drop null values
    # 2. drop remove "" 
    df_1 = pd.read_csv(csv_file_name_1)
    df_2 = pd.read_csv(csv_file_name_2, delimiter=';')
    #df_3 = pd.read_csv(csv_file_name_3, delimiter=',', skiprows=lambda x: x not in [0])
    #columns_3 = list(df_3.columns)
    df_3 =  pd.read_csv(csv_file_name_3, delimiter=',|;|\t', engine='python')
    
    df_1.dropna(inplace=True)
    df_2.dropna(inplace=True)
    df_3.dropna(inplace=True)

    for i in range(len(df_1)):
        fname = df_1.iloc[i]['FirstName']
        lname = df_1.iloc[i]['LastName']
        df_1.iat[i, 1] = re.sub(r'[\W_]', '', fname).title()
        df_1.iat[i, 2] = re.sub(r'[\W_]', '', lname).title()

        sex = df_1.iloc[i]['Gender']
        if sex == '0':
            df_1.iat[i, 0] = 'Female'
            
        elif sex == '1':
            df_1.iat[i, 0] = 'Male'
            
        elif sex == 'F':
            df_1.iat[i, 0] = 'Female'

        elif sex == 'M':
            df_1.iat[i, 0] = 'Male'

        country = df_1.iloc[i]['Country']
        if country == 'United State Of America' or country == 'U.S.A.' or country == '12' or country == 'US':
            df_1.iat[i, 7] = 'USA'

        city = df_1.iloc[i]['City']
        df_1.iat[i, 6] = re.sub(r'[\W_]', ' ', city).title()


    for i in range(len(df_1)):
        fname = df_1.iloc[i]['FirstName']
        lname = df_1.iloc[i]['LastName']
        df_1.iat[i, 1] = re.sub(r'[\W_]', '', fname).title()
        df_1.iat[i, 2] = re.sub(r'[\W_]', '', lname).title()

        sex = df_1.iloc[i]['Gender']
        if sex == '0':
            df_1.iat[i, 0] = 'Female'
            
        elif sex == '1':
            df_1.iat[i, 0] = 'Male'
            
        elif sex == 'F':
            df_1.iat[i, 0] = 'Female'

        elif sex == 'M':
            df_1.iat[i, 0] = 'Male'

        country = df_1.iloc[i]['Country']
        if country == 'United State Of America' or country == 'U.S.A.' or country == '12' or country == 'US':
            df_1.iat[i, 7] = 'USA'

        city = df_1.iloc[i]['City']
        df_1.iat[i, 6] = re.sub(r'[\W_]', ' ', city).title()
    
    # add column names to the 2nd df (2nd csv file)
    df_2.columns = ['Age','City','Gender','FullName', 'Email']
    df_2['Country'] = 'USA'
    df_2['UserName'] = 'NaN'
    df_2['FirstName'] = 'NaN'
    df_2['LastName'] = 'NaN'
    df_2 = df_2[['Gender', 'FirstName', 'LastName', 'FullName', 'UserName', 'Email', 'Age', 'City', 'Country']]
    
    for i in range(len(df_2)): 
        fname, lname = [re.sub(r'[\W_]', '', name).title() for name in df_2.iloc[i]['FullName'].split()]
        df_2.iat[i, 1] = fname
        df_2.iat[i, 2] = lname

        username = fname.lower()
        df_2.iat[i,4] = username
        
        sex = df_2.iloc[i]['Gender']
        if sex == '0':
            df_2.iat[i, 0] = 'Female'
            
        elif sex == '1':
            df_2.iat[i, 0] = 'Male'
            
        elif sex == 'F':
            df_2.iat[i, 0] = 'Female'

        elif sex == 'M':
            df_2.iat[i, 0] = 'Male'
            
        age = df_2.iloc[i]['Age']
        df_2.iat[i, 6] = re.search('[0-9]+', age)[0]
        
        city = df_2.iloc[i]['City']
        df_2.iat[i, 7] = re.sub(r'[\W_]', ' ', city).title()
        
    df_2.drop(columns=['FullName'], inplace=True)
    
    df_3['UserName'] = 'NaN'
    df_3['Country'] = 'USA'
    df_3['FirstName'] = 'NaN'
    df_3['LastName'] = 'NaN'
    
    for i in range(len(df_3)): 
        sex_str = df_3.iloc[i]['Gender']
        sex = re.findall('_(.+)', sex_str)[0]
        if sex == 'M' or sex == '1' or sex == 'Male':
            df_3.iat[i, 0] = 'Male'
        elif sex == 'F' or sex == '0' or sex == 'Female':
            df_3.iat[i, 0] = 'Female'

        name = df_3.iloc[i]['Name']
        full_name = re.findall('string_(.+)', name)[0].split()
        fname = re.sub(r'[\W_]', '', full_name[0]).title()
        lname = re.sub(r'[\W_]', '', full_name[1]).title()
        user_name = lname.lower()
        df_3.iat[i,7] = fname
        df_3.iat[i,8] = lname
        df_3.iat[i,6] = user_name

        age_str = df_3.iloc[i]['Age']
        age = re.search('[0-9]+', age_str)[0]
        df_3.iat[i,3] = age

        city_str = df_3.iloc[i]['City']
        city = re.findall('string_(.+)', city_str)[0]
        city = re.sub(r'[\W_]', ' ', city).title()
        df_3.iat[i,4] = city 

        email_str = df_3.iloc[i]['Email']
        email = re.findall('_(.+)', email_str)[0]
        df_3.iat[i,2] = email 
        
    df_3.drop(columns=['Name'], inplace=True)
    df_3 = df_3[['Gender', 'FirstName', 'LastName', 'UserName', 'Email', 'Age', 'City', 'Country']]

    # concatenating dataframes 
    df_merged = pd.concat([df_1,df_2,df_3], ignore_index=True)

    # wrte df to a csv file
    df_merged.to_csv('merged_csv.csv', index=False)
    merged_csv_file = open('merged_csv.csv')
    return merged_csv_file
    
def my_m_and_a():
    merged_csv = clean_up("only_wood_customer_us_1.csv","only_wood_customer_us_2.csv","only_wood_customer_us_3.csv")
    my_ds_babel.csv_to_sql(merged_csv, 'plastic_free_boutique.sql', 'customers')