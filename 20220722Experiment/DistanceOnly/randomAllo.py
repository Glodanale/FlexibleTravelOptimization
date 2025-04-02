import pandas as pd
import numpy as np
import mysql.connector
import clusterSets

def pullChildrenFromMySQL():
    connection = mysql.connector.connect(
            host = "localhost",
            port = 3306,
            user = 'root',
            database = "expanding_horizons",
            password = "dB79*dG2024!"
    )
    
    cursor = connection.cursor()
    
    query = ("SELECT child_id, First_Name, Last_Name FROM child ")
    cursor.execute(query)
    
    rows = cursor.fetchall()
    
    column_names = ['child_id', 'First_Name', 'Last_Name']

    data = pd.DataFrame(rows, columns=column_names)
    
    cursor.close()
    connection.close()
    
    return data

def pullEIFromMySQL():
    connection = mysql.connector.connect(
            host = "localhost",
            port = 3306,
            user = 'root',
            database = "expanding_horizons",
            password = "dB79*dG2024!"
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

def pushToMySQL(shuffled_children, table):
    connection = mysql.connector.connect(
            host = "localhost",
            port = 3306,
            user = 'root',
            database = "expanding_horizons",
            password = "dB79*dG2024!"
    )
    
    cursor = connection.cursor()
    
    query = (f"UPDATE {table} "
             "SET assigned = 0")
    cursor.execute(query)
    connection.commit()
    
    for index, row in shuffled_children.iterrows():
        child_id = row["child_id"]
        ei_id = row["Assigned_EI"]
        updateQuery = (f"UPDATE {table} " 
                       f"SET assigned = 1 " 
                       f"WHERE child_id_1 = '{child_id}' and ei_id_1 = '{ei_id}'")
        cursor.execute(updateQuery)
        connection.commit()
    
    cursor.close()
    connection.close()
    
    
def randomUniformCap(table):
    child = pullChildrenFromMySQL()
    shuffled_children = child.sample(frac=1)
    shuffled_children.reset_index(drop=True, inplace=True)
    shuffled_children["Assigned_EI"] = ""    
    ei = pullEIFromMySQL()

    for index, row in shuffled_children.iterrows():
        value = index % 12
        ei_value = ei.iloc[value, 0]
        shuffled_children.at[index, 'Assigned_EI'] = ei_value
        
    pushToMySQL(shuffled_children, table)

def randomRealCap(table):
    eiSet = clusterSets.eiAllocationQuantity()
    child = pullChildrenFromMySQL()
    shuffled_children = child.sample(frac=1)
    shuffled_children.reset_index(drop=True, inplace=True)
    shuffled_children["Assigned_EI"] = ""

    min = 0
    for index, row in eiSet.iterrows():
        eiSet.at[index, "Range_Min"] = min
        rangeMax = min + row["Count_Assigned"] - 1
        eiSet.at[index, "Range_Max"] = rangeMax
        min = rangeMax + 1

    for indexC, rowC in shuffled_children.iterrows():
        for indexE, rowE in eiSet.iterrows():
            if indexC >= rowE["Range_Min"] and indexC <= rowE["Range_Max"]:
                shuffled_children.at[indexC, "Assigned_EI"] = rowE["ei_id"]

    pushToMySQL(shuffled_children, table)