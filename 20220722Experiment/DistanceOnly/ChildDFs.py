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
        child1_child_query = ("SELECT child_id_1, child_id_2, driving_distance "
                              "FROM locationdata "
                              f"WHERE child_id_1 = '{row[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child1_child_query)
        child1_child = cursor.fetchall()

        for row2 in child1_child:
            # Add columns with prefixes
            data[f'{row2[1]}_distance'] = row2[2]

        child_child2_query = ("SELECT child_id_1, child_id_2, driving_distance "
                              "FROM locationdata "
                              f"WHERE child_id_2 = '{row[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child_child2_query)
        child_child2 = cursor.fetchall()

        for row3 in child_child2:
            data[f'{row3[0]}_distance'] = row3[2]

        child_ei_query = ("SELECT child_id_1, ei_id_1, driving_distance "
                          "FROM locationdata "
                          f"WHERE child_id_1 = '{row[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child_ei_query)
        child_ei = cursor.fetchall()

        for row4 in child_ei:
            data[f'{row4[1]}_distance'] = row4[2]

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
        child1_child_query = ("SELECT child_id_1, child_id_2, driving_distance "
                              "FROM locationdata "
                              f"WHERE child_id_1 = '{row[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child1_child_query)
        child1_child = cursor.fetchall()

        for row2 in child1_child:
            # Add columns with prefixes
            data[f'{row2[1]}_distance'] = row2[2]

        child_child2_query = ("SELECT child_id_1, child_id_2, driving_distance "
                              "FROM locationdata "
                              f"WHERE child_id_2 = '{row[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child_child2_query)
        child_child2 = cursor.fetchall()

        for row3 in child_child2:
            data[f'{row3[0]}_distance'] = row3[2]

        child_ei_query = ("SELECT child_id_1, ei_id_1, driving_distance "
                          "FROM locationdata "
                          f"WHERE child_id_1 = '{row[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child_ei_query)
        child_ei = cursor.fetchall()

        for row4 in child_ei:
            data[f'{row4[1]}_distance'] = row4[2]

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
        child1_child_query = ("SELECT child_id_1, child_id_2, driving_distance "
                              "FROM locationdata "
                              f"WHERE child_id_1 = '{row[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child1_child_query)
        child1_child = cursor.fetchall()

        for row2 in child1_child:
            # Add columns with prefixes
            data[f'{row2[1]}_distance'] = row2[2]

        child_child2_query = ("SELECT child_id_1, child_id_2, driving_distance "
                              "FROM locationdata "
                              f"WHERE child_id_2 = '{row[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child_child2_query)
        child_child2 = cursor.fetchall()

        for row3 in child_child2:
            data[f'{row3[0]}_distance'] = row3[2]

        child_ei_query = ("SELECT child_id_1, ei_id_1, driving_distance "
                          "FROM locationdata "
                          f"WHERE child_id_1 = '{row[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child_ei_query)
        child_ei = cursor.fetchall()

        for row4 in child_ei:
            data[f'{row4[1]}_distance'] = row4[2]

        data_list.append(data)

    childDataFrame = pd.DataFrame(data_list)
    childDataFrame["assigned"] = ""
    childDataFrame = childDataFrame.fillna(0)
    
    childDataFrameOrdered = childDataFrame.sort_index(axis=1)
    
    cursor.close()
    connection.close()

    return childDataFrameOrdered

