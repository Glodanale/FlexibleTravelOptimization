import pandas as pd
import numpy as np
import mysql.connector

def calculateSlope(o_lat, o_long, d_lat, d_long):
    y = d_long - o_long
    x = d_lat - o_lat

    if y >= 0:
        long = 1
    else:
        long = -1

    if x >= 0:
        lat = 1
    else:
        lat = -1

    if x == 0 or y == 0:
        slope = 0
    else:
        slope = y/x

    return slope, lat, long

def calculateCoord(d, m, lat, long):
    x = d / np.sqrt(1 + m**2)
    x = x * lat

    y = d * m / np.sqrt(1 + m**2)
    y = y * long
    
    return x, y


def assembleDF(child_ids, cursor):
    data_list = []
    for row in child_ids:
        data = {}
        data["id"] = row[0]
        origin_long = row[1]
        origin_lat = row[2]
        data["longitude"] = origin_long
        data["latitude"] = origin_lat
        child1_child_query = ("SELECT ld.child_id_1, ld.child_id_2, driving_distance, driving_time, child.Longitude, child.Latitude "
                              "FROM locationdata ld "
                              "JOIN child on ld.child_id_2 = child.child_id "
                              f"WHERE child_id_1 = '{row[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child1_child_query)
        child1_child = cursor.fetchall()

        for subRow in child1_child:
            dest_id = subRow[1]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = calculateCoord(driveDist, slope, lat, long)

            # Add columns with prefixes
            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        child_child2_query = ("SELECT ld.child_id_1, ld.child_id_2, driving_distance, driving_time, child.Longitude, child.Latitude "
                              "FROM locationdata ld "
                              "JOIN child on ld.child_id_1 = child.child_id "
                              f"WHERE child_id_2 = '{row[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child_child2_query)
        child_child2 = cursor.fetchall()

        for subRow in child_child2:
            dest_id = subRow[0]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = calculateCoord(driveDist, slope, lat, long)

            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        child_ei_query = ("SELECT child_id_1, ei_id_1, driving_distance, driving_time, ei.Longitude, ei.Latitude "
                          "FROM locationdata ld "
                          "JOIN ei on ld.ei_id_1 = ei.ei_id "
                          f"WHERE child_id_1 = '{row[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child_ei_query)
        child_ei = cursor.fetchall()

        for subRow in child_ei:
            dest_id = subRow[1]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = calculateCoord(driveDist, slope, lat, long)

            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        data_list.append(data)

    childDataFrame = pd.DataFrame(data_list)

    return childDataFrame


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

    childDataFrame = assembleDF(child_ids, cursor)
    
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

    childDataFrame = assembleDF(child_ids, cursor)
    
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

    childDataFrame = assembleDF(child_ids, cursor)

    childDataFrame["assigned"] = ""
    childDataFrame = childDataFrame.fillna(0)
    childDataFrameOrdered = childDataFrame.sort_index(axis=1)
    
    cursor.close()
    connection.close()

    return childDataFrameOrdered

