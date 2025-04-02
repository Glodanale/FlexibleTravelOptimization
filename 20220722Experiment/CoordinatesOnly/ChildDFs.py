import pandas as pd
import numpy as np
import mysql.connector

def childDFRandomSort():
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expanding_horizons",
                password = "dB79*dG2024!"
    )

    cursor = connection.cursor()
    
    query = ("SELECT child_id, longitude, latitude FROM child ")
    cursor.execute(query)

    child_ids = cursor.fetchall()

    data_list = []
    for row in child_ids:
        data = {}
        data["id"] = row[0]
        data["longitude"] = row[1]
        data["latitude"] = row[2]
        data_list.append(data)

    childDataFrame = pd.DataFrame(data_list)
    childDataFrame = childDataFrame.sample(frac=1)  #shuffle children
    childDataFrame.reset_index(drop=True, inplace=True)
    childDataFrame["assigned"] = ""
    childDataFrame = childDataFrame.fillna(0)
    
    childDataFrameOrdered = childDataFrame.sort_index(axis=1)
    
    cursor.close()
    connection.close()

    return childDataFrameOrdered

def childDFDistanceSort():
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expanding_horizons",
                password = "dB79*dG2024!"
    )

    cursor = connection.cursor()
    
    query = ("Select child_id_1, child.longitude, child.latitude, min(driving_distance) shortest_distance "
             "from locationdata ld "
             "join child on child.child_id = ld.child_id_1 "
             "where child_id_2 is NULL and ei_id_2 is NULL "
             "group by child_id_1, child.longitude, child.latitude "
             "order by shortest_distance desc")
    cursor.execute(query)

    child_ids = cursor.fetchall()

    data_list = []
    for row in child_ids:
        data = {}
        data["id"] = row[0]
        data["longitude"] = row[1]
        data["latitude"] = row[2]

        data_list.append(data)

    childDataFrame = pd.DataFrame(data_list)
    childDataFrame["assigned"] = ""
    childDataFrame = childDataFrame.fillna(0)
    
    childDataFrameOrdered = childDataFrame.sort_index(axis=1)
    
    cursor.close()
    connection.close()

    return childDataFrameOrdered

def childDFZsort():
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expanding_horizons",
                password = "dB79*dG2024!"
    )

    cursor = connection.cursor()

    query = ("Select child_id_1, child.Longitude, child.Latitude, ((min(driving_distance) - avg(driving_distance)) / std(driving_distance)) as z_score "
             "from locationdata ld "
             "join child on child.child_id = ld.child_id_1 "
             "where child_id_2 is NULL and ei_id_2 is NULL "
             "group by child_id_1, child.longitude, child.latitude "
             "order by z_score asc")
    cursor.execute(query)

    child_ids = cursor.fetchall()

    data_list = []
    for row in child_ids:
        data = {}
        data["id"] = row[0]
        data["longitude"] = row[1]
        data["latitude"] = row[2]

        data_list.append(data)

    childDataFrame = pd.DataFrame(data_list)
    childDataFrame["assigned"] = ""
    childDataFrame = childDataFrame.fillna(0)
    
    childDataFrameOrdered = childDataFrame.sort_index(axis=1)
    
    cursor.close()
    connection.close()

    return childDataFrameOrdered

