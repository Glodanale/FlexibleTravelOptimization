import pandas as pd
import numpy as np
import mysql.connector

def pullDataFromMySQL():
    connection = mysql.connector.connect(
            host = "localhost",
            port = 3306,
            user = 'root',
            database = "expandinghorizons",
            password = "dB79@dG2024!"
    )
    
    cursor = connection.cursor()
    
    query = ("SELECT * FROM locationdatarandom "
             "Where child_id_2 is null and ei_id_2 is null")
    cursor.execute(query)
    
    rows = cursor.fetchall()
    
    column_names = ['pair_id', 'child_id_1', 'child_id_2', 'ei_id_1', 'ei_id_2', 'assigned', 'driving_distance', 'driving_time']

    # Create DataFrame
    data = pd.DataFrame(rows, columns=column_names)
    
    #print(data)
    
    cursor.close()
    connection.close()
    
    return data

def pullChildrenFromMySQL():
    connection = mysql.connector.connect(
            host = "localhost",
            port = 3306,
            user = 'root',
            database = "expandinghorizons",
            password = "dB79@dG2024!"
    )
    
    cursor = connection.cursor()
    
    query = ("SELECT child_id, First_Name, Last_Name FROM child ")
    cursor.execute(query)
    
    rows = cursor.fetchall()
    
    column_names = ['child_id', 'First_Name', 'Last_Name']

    # Create DataFrame
    data = pd.DataFrame(rows, columns=column_names)
    
    #print(data)
    
    cursor.close()
    connection.close()
    
    return data

def pullEIFromMySQL():
    connection = mysql.connector.connect(
            host = "localhost",
            port = 3306,
            user = 'root',
            database = "expandinghorizons",
            password = "dB79@dG2024!"
    )
    
    cursor = connection.cursor()
    
    query = ("SELECT ei_id, First_Name, Last_Name FROM ei "
             "WHERE ei_id != '65d75e28e09e67e025e8cd3c'")
    cursor.execute(query)
    
    rows = cursor.fetchall()
    
    column_names = ['ei_id', 'First_Name', 'Last_Name']

    # Create DataFrame
    data = pd.DataFrame(rows, columns=column_names)
    
    #print(data)
    
    cursor.close()
    connection.close()
    
    return data

def pushToMySQL(shuffled_children):
    connection = mysql.connector.connect(
            host = "localhost",
            port = 3306,
            user = 'root',
            database = "expandinghorizons",
            password = "dB79@dG2024!"
    )
    
    cursor = connection.cursor()
    
    query = ("UPDATE locationdatarandom "
             "SET assigned = 0")
    cursor.execute(query)
    connection.commit()
    
    for index, row in shuffled_children.iterrows():
        child_id = row["child_id"]
        ei_id = row["Assigned_EI"]
        updateQuery = ("UPDATE locationdatarandom " 
                       f"SET assigned = 1 " 
                       f"WHERE child_id_1 = '{child_id}' and ei_id_1 = '{ei_id}'")
        cursor.execute(updateQuery)
        #print(f"{row['First_Name']} {row['Last_Name']} updated assignement")
        connection.commit()
    
    cursor.close()
    connection.close()
    #print("\n\n")
    #print("End of function")
    
    
def execute():
    child = pullChildrenFromMySQL()
    shuffled_children = child.sample(frac=1)
    shuffled_children.reset_index(drop=True, inplace=True)
    shuffled_children["Assigned_EI"] = ""    
    ei = pullEIFromMySQL()
    for index, row in shuffled_children.iterrows():
        value = index % 12
        ei_value = ei.iloc[value, 0]
        row['Assigned_EI'] = ei_value
        
    pushToMySQL(shuffled_children)