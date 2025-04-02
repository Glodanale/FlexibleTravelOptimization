import pandas as pd
import numpy as np
import ChildDFs
import mysql.connector

def eiAllocationQuantity():
    connection1 = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expanding_horizons",
                password = "dB79*dG2024!"
    )

    cursor1 = connection1.cursor()
    
    query1 = ("Select ei_id_1, count(assigned) Count_Assigned " 
              "from locationdatareal "
              "where assigned = 1 "
              "group by ei_id_1")
    
    cursor1.execute(query1)
    ei_set = cursor1.fetchall()
    
    cursor1.close()
    connection1.close()
    
    ei_set_df = pd.DataFrame(ei_set, columns=["ei_id", "Count_Assigned"])
    print(type(ei_set_df))

    return ei_set_df


def eiLocationClusters():
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expanding_horizons",
                password = "dB79*dG2024!"
    )

    cursor = connection.cursor()
    
    query = ("Select ei_id, longitude, latitude from ei ")
    cursor.execute(query)

    ei_ids = cursor.fetchall()

    ei_list = []
    for row in ei_ids:
        data = {}
        data["id"] = row[0]
        origin_long = row[1]
        origin_lat = row[2]
        data["longitude"] = origin_long
        data["latitude"] = origin_lat
        ei_child_query = ("SELECT child_id_1, ei_id_1, driving_distance, driving_time, child.Longitude, child.Latitude "
                          "FROM locationdata ld "
                          "JOIN child on ld.child_id_1 = child.child_id "
                          f"WHERE ei_id_1 = '{row[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(ei_child_query)
        ei_child = cursor.fetchall()

        for subRow in ei_child:
            dest_id = subRow[0]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

            # Add columns with prefixes
            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        ei1_ei_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time, ei.Longitude, ei.Latitude "
                        "FROM locationdata ld "
                        "JOIN ei on ld.ei_id_2 = ei.ei_id "
                        f"WHERE ei_id_1 = '{row[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei1_ei_query)
        ei1_ei = cursor.fetchall()

        for subRow in ei1_ei:
            dest_id = subRow[1]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

            # Add columns with prefixes
            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        ei_ei2_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time, ei.Longitude, ei.Latitude "
                        "FROM locationdata ld "
                        "JOIN ei on ld.ei_id_1 = ei.ei_id "
                        f"WHERE ei_id_2 = '{row[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei_ei2_query)
        ei_ei2 = cursor.fetchall()

        for subRow in ei_ei2:
            dest_id = subRow[0]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

            # Add columns with prefixes
            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        ei_list.append(data)

    eiDataFrame = pd.DataFrame(ei_list)
    eiDataFrame["assignment_count"] = 0
    eiDataFrame = eiDataFrame.fillna(0)
    
    eiDataFrameOrdered = eiDataFrame.sort_index(axis=1)
    
    cursor.close()
    connection.close()
    return eiDataFrameOrdered

def clusterRealUniform():
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expanding_horizons",
                password = "dB79*dG2024!"
    )

    cursor = connection.cursor()

    query = ("Select ei_id, longitude, latitude from ei ")
    cursor.execute(query)
    ei_ids = cursor.fetchall()
    eiDFs = []

    for row in ei_ids:
        ei_group_list = []
        data = {}
        data["id"] = row[0]
        origin_long = row[1]
        origin_lat = row[2]
        data["longitude"] = origin_long
        data["latitude"] = origin_lat
        ei_child_query = ("SELECT child_id_1, ei_id_1, driving_distance, driving_time, child.Longitude, child.Latitude "
                          "FROM locationdata ld "
                          "JOIN child on ld.child_id_1 = child.child_id "
                          f"WHERE ei_id_1 = '{row[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(ei_child_query)
        ei_child = cursor.fetchall()

        for subRow in ei_child:
            dest_id = subRow[0]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

            # Add columns with prefixes
            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        ei1_ei_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time, ei.Longitude, ei.Latitude "
                        "FROM locationdata ld "
                        "JOIN ei on ld.ei_id_2 = ei.ei_id "
                        f"WHERE ei_id_1 = '{row[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei1_ei_query)
        ei1_ei = cursor.fetchall()

        for subRow in ei1_ei:
            dest_id = subRow[1]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

            # Add columns with prefixes
            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        ei_ei2_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time, ei.Longitude, ei.Latitude "
                        "FROM locationdata ld "
                        "JOIN ei on ld.ei_id_1 = ei.ei_id "
                        f"WHERE ei_id_2 = '{row[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei_ei2_query)
        ei_ei2 = cursor.fetchall()

        for subRow in ei_ei2:
            dest_id = subRow[0]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

            # Add columns with prefixes
            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        ei_group_list.append(data)

        childQuery = ("SELECT child.child_id, child.longitude, child.latitude FROM locationdatareal ld "
                      "join child on child.child_id = ld.child_id_1 "
                      f"where child_id_2 is null and ei_id_2 is null and ld.assigned = 1 and ei_id_1 = '{row[0]}'")

        cursor.execute(childQuery)
        assignedChild = cursor.fetchall()

        for row in assignedChild:
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

                slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
                x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

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

                slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
                x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

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

                slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
                x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

                data[f'{dest_id}_distanceX'] = x
                data[f'{dest_id}_distanceY'] = y
                data[f'{dest_id}_time'] = driveTime

            
            ei_group_list.append(data)
        eiDataFrame = pd.DataFrame(ei_group_list)
        eiDataFrame = eiDataFrame.fillna(0)
        eiDFs.append(eiDataFrame)
        
    clusterCenters = pd.DataFrame()
    for df in eiDFs:
        id_value = df.loc[0, "id"]
        df = df.drop(columns=["id"])
        #print(df)
        new_row = df.mean()
        new_row["id"] = id_value
        new_row["assignment_count"] = 0
        new_row = new_row.to_frame()
        new_row = new_row.transpose()
        clusterCenters = pd.concat([clusterCenters, new_row], ignore_index = True)
        
    clusterCentersOrdered = clusterCenters.sort_index(axis=1)
        
    cursor.close()
    connection.close()

    return clusterCentersOrdered

def clusterRealWeighted():
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expanding_horizons",
                password = "dB79*dG2024!"
    )

    cursor = connection.cursor()

    query = ("Select ei_id, longitude, latitude from ei ")
    cursor.execute(query)

    ei_ids = cursor.fetchall()

    eiDFs = []

    for row in ei_ids:
        ei_group_list = []
        data = {}
        data["id"] = row[0]
        origin_long = row[1]
        origin_lat = row[2]
        data["longitude"] = origin_long
        data["latitude"] = origin_lat
        ei_child_query = ("SELECT child_id_1, ei_id_1, driving_distance, driving_time, child.Longitude, child.Latitude "
                          "FROM locationdata ld "
                          "JOIN child on ld.child_id_1 = child.child_id "
                          f"WHERE ei_id_1 = '{row[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(ei_child_query)
        ei_child = cursor.fetchall()

        for subRow in ei_child:
            dest_id = subRow[0]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

            # Add columns with prefixes
            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        ei1_ei_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time, ei.Longitude, ei.Latitude "
                        "FROM locationdata ld "
                        "JOIN ei on ld.ei_id_2 = ei.ei_id "
                        f"WHERE ei_id_1 = '{row[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei1_ei_query)
        ei1_ei = cursor.fetchall()

        for subRow in ei1_ei:
            dest_id = subRow[1]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

            # Add columns with prefixes
            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        ei_ei2_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time, ei.Longitude, ei.Latitude "
                        "FROM locationdata ld "
                        "JOIN ei on ld.ei_id_1 = ei.ei_id "
                        f"WHERE ei_id_2 = '{row[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei_ei2_query)
        ei_ei2 = cursor.fetchall()

        for subRow in ei_ei2:
            dest_id = subRow[0]
            driveDist = subRow[2]
            driveTime = subRow[3]
            dest_long = subRow[4]
            dest_lat = subRow[5]

            slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
            x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

            # Add columns with prefixes
            data[f'{dest_id}_distanceX'] = x
            data[f'{dest_id}_distanceY'] = y
            data[f'{dest_id}_time'] = driveTime

        ei_group_list.append(data)

        childQuery = ("SELECT child.child_id, child.longitude, child.latitude FROM locationdatareal ld "
                      "join child on child.child_id = ld.child_id_1 "
                      f"where child_id_2 is null and ei_id_2 is null and ld.assigned = 1 and ei_id_1 = '{row[0]}'")

        cursor.execute(childQuery)
        assignedChild = cursor.fetchall()

        for row in assignedChild:
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

                slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
                x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

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

                slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
                x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

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

                slope, lat, long = ChildDFs.calculateSlope(origin_lat, origin_long, dest_lat, dest_long)
                x, y = ChildDFs.calculateCoord(driveDist, slope, lat, long)

                data[f'{dest_id}_distanceX'] = x
                data[f'{dest_id}_distanceY'] = y
                data[f'{dest_id}_time'] = driveTime

            
            ei_group_list.append(data)
        eiDataFrame = pd.DataFrame(ei_group_list)
        eiDataFrame = eiDataFrame.fillna(0)
        eiDFs.append(eiDataFrame)
        
    clusterCenters = pd.DataFrame()
    for df in eiDFs:
        for index, row in df.iterrows():
            ei_row = row
            ei_row = ei_row.to_frame()
            ei_row = ei_row.transpose()
            break
        id_value = df.loc[0, "id"]
        df = df.drop(columns=["id"])
        new_row = df.mean()
        new_row = new_row.to_frame()
        new_row = new_row.transpose()

        meanDF = pd.concat([ei_row, new_row], ignore_index = True)
        meanDF = meanDF.drop(columns=["id"])
        clusterCenterPoint = meanDF.mean()
        clusterCenterPoint["id"] = id_value
        clusterCenterPoint["assignment_count"] = 0
        clusterCenterPoint = clusterCenterPoint.to_frame()
        clusterCenterPoint = clusterCenterPoint.transpose()
        clusterCenters = pd.concat([clusterCenters, clusterCenterPoint], ignore_index = True)
        
    clusterCentersOrdered = clusterCenters.sort_index(axis=1)
        
    cursor.close()
    connection.close()

    return clusterCentersOrdered