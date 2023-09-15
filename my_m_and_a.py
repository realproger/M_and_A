import pandas as pd
import my_ds_babel

def my_m_and_a(customers_1, customers_2, customers_3):

    df_csv_1 = pd.read_csv(customers_1)

    # DATA PREPROCESSING
    df_csv_1.drop(["UserName"], inplace=True, axis=1)
    df_csv_1["Gender"] = df_csv_1['Gender'].replace({'^M$|^1$': 'Male', '^F$|^0$': 'Female'}, regex=True)

    # df_csv_1["FirstName"] = df_csv_1['FirstName'].replace(r'["\\]', '', regex=True).str.title()
    # df_csv_1["LastName"] = df_csv_1['LastName'].replace(r'["\\]', '', regex=True).str.title()

    df_csv_1["FirstName"] = df_csv_1['FirstName'].apply(lambda val: str(val)).str.title()
    df_csv_1["LastName"] = df_csv_1['LastName'].apply(lambda val: str(val)).str.title()
    df_csv_1["Age"] = df_csv_1['Age'].astype(str)
    df_csv_1["Email"] = df_csv_1['Email'].str.lower()
    df_csv_1["City"] = df_csv_1['City'].replace(r'[-_]', ' ', regex=True).str.title()
    df_csv_1["Country"] = "USA"
    
    ####                           ####
####    # only wood customer us 2 #    ####
    ####                           ####
    
    df_csv_2 = pd.read_csv(customers_2, sep=';')
    df_csv_2.columns = ["Age", "City", "Gender", "Name", "Email"]

    df_csv_2["Age"] = df_csv_2['Age'].replace(r'[years{0,1}|yo]', '', regex=True).astype(str)
    df_csv_2["City"] = df_csv_2['City'].replace(r'[-_]', ' ', regex=True).str.title()
    df_csv_2["Gender"] = df_csv_2['Gender'].replace({'^M$|^1$': 'Male', '^F$|^0$': 'Female'}, regex=True)
    df_csv_2[["FirstName", "LastName"]] = df_csv_2.Name.str.split(pat=' ', expand=True)
    df_csv_2.drop(["Name"], inplace=True, axis=1)

    # df_csv_2["FirstName"] = df_csv_2['FirstName'].replace(r'["\\]', '', regex=True).str.title().astype(str)
    # df_csv_2["LastName"] = df_csv_2['LastName'].replace(r'["\\]', '', regex=True).str.title().astype(str)

    df_csv_2["FirstName"] = df_csv_2['FirstName'].apply(lambda val: str(val)).str.title()
    df_csv_2["LastName"] = df_csv_2['LastName'].apply(lambda val: str(val)).str.title()
    df_csv_2["Email"] = df_csv_2['Email'].str.lower()
    df_csv_2["Country"] = "USA"

    ####                           ####
####    # only wood customer us 3 #    ####
    ####                           ####

    df_csv_3 = pd.read_csv(customers_3, sep='\t|,', engine='python')

    df_csv_3["Gender"] = df_csv_3['Gender'].replace({'^string_Male$|^boolean_1$|^character_M$': 'Male', '^string_Female$|^boolean_0$': 'Female'}, regex=True)
    df_csv_3[["FirstName", "LastName"]] = df_csv_3['Name'].str.split(pat=' ', expand=True)

    # df_csv_3["FirstName"] = df_csv_3['FirstName'].replace(r'["\\|string_]', '', regex=True).str.title().astype(str)
    df_csv_3["FirstName"] = df_csv_3['FirstName'].apply(lambda v: str(v) if not isinstance(v, str) else v).str.title()
    # df_csv_3["LastName"] = df_csv_3['LastName'].replace(r'["\\]', '', regex=True).str.title().astype(str)
    df_csv_3["LastName"] = df_csv_3['LastName'].apply(lambda ln: str(ln) if not pd.isna(ln) else ln).str.title()
    df_csv_3.drop(["Name"], inplace=True, axis=1)

    df_csv_3["Email"] = df_csv_3['Email'].replace(r'[string_]', '', regex=True).str.lower()
    df_csv_3["Age"] = df_csv_3['Age'].replace(r'["integer_|years{0,1}|yo"]', '', regex=True).astype(str)
    df_csv_3["City"] = df_csv_3['City'].replace(r'^string_|-_', '', regex=True).str.title()
    df_csv_3["Country"] = "USA"

    combining_df = pd.concat([df_csv_1, df_csv_2, df_csv_3], ignore_index=True)

    return combining_df
merged_csv = my_m_and_a('only_wood_customer_us_1.csv', 'only_wood_customer_us_2.csv', 'only_wood_customer_us_3.csv')
merged_csv.to_csv('final_df.csv', index=False)
my_ds_babel.csv_to_sql('final_df.csv', 'plastic_free_boutique.sql', 'customers')
