import pandas as pd
import numpy as np
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
        data["longitude"] = row[1]
        data["latitude"] = row[2]
        ei_child_query = ("SELECT child_id_1, ei_id_1, driving_distance, driving_time "
                          "FROM locationdata "
                          f"WHERE ei_id_1 = '{row[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(ei_child_query)
        ei_child = cursor.fetchall()

        for row2 in ei_child:
            # Add columns with prefixes
            data[f'{row2[0]}_distance'] = row2[2]
            data[f'{row2[0]}_time'] = row2[3]

        ei1_ei_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time "
                        "FROM locationdata "
                        f"WHERE ei_id_1 = '{row[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei1_ei_query)
        ei1_ei = cursor.fetchall()

        for row3 in ei1_ei:
            data[f'{row3[1]}_distance'] = row3[2]
            data[f'{row3[1]}_time'] = row3[3]

        ei_ei2_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time "
                        "FROM locationdata "
                        f"WHERE ei_id_2 = '{row[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei_ei2_query)
        ei_ei2 = cursor.fetchall()

        for row4 in ei_ei2:
            data[f'{row4[0]}_distance'] = row4[2]
            data[f'{row4[0]}_time'] = row4[3]

        ei_list.append(data)

    eiDataFrame = pd.DataFrame(ei_list)
    eiDataFrame = eiDataFrame[eiDataFrame['id'] != '65d75e28e09e67e025e8cd3c']  #drop this EI because she is currently not seeing children
    eiDataFrame["assignment_count"] = 1
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

    query = ("Select ei_id, longitude, latitude from ei "
             "WHERE ei_id != '65d75e28e09e67e025e8cd3c'")
    cursor.execute(query)
    ei_ids = cursor.fetchall()
    eiDFs = []

    for ei in ei_ids:
        ei_group_list = []
        data = {}
        data["id"] = ei[0]
        data["longitude"] = ei[1]
        data["latitude"] = ei[2]
        ei_child_query = ("SELECT child_id_1, ei_id_1, driving_distance, driving_time "
                          "FROM locationdata "
                          f"WHERE ei_id_1 = '{ei[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(ei_child_query)
        ei_child = cursor.fetchall()

        for row2 in ei_child:
            # Add columns with prefixes
            data[f'{row2[0]}_distance'] = row2[2]
            data[f'{row2[0]}_time'] = row2[3]

        ei1_ei_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time "
                        "FROM locationdata "
                        f"WHERE ei_id_1 = '{ei[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei1_ei_query)
        ei1_ei = cursor.fetchall()

        for row3 in ei1_ei:
            data[f'{row3[1]}_distance'] = row3[2]
            data[f'{row3[1]}_time'] = row3[3]

        ei_ei2_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time "
                        "FROM locationdata "
                        f"WHERE ei_id_2 = '{ei[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei_ei2_query)
        ei_ei2 = cursor.fetchall()

        for row4 in ei_ei2:
            data[f'{row4[0]}_distance'] = row4[2]
            data[f'{row4[0]}_time'] = row4[3]

        ei_group_list.append(data)

        childQuery = ("SELECT child.child_id, child.longitude, child.latitude FROM locationdatareal ld "
                      "join child on child.child_id = ld.child_id_1 "
                      f"where child_id_2 is null and ei_id_2 is null and ld.assigned = 1 and ei_id_1 = '{ei[0]}'")

        cursor.execute(childQuery)
        assignedChild = cursor.fetchall()

        for child in assignedChild:
            data = {}
            data["id"] = child[0]
            data["longitude"] = child[1]
            data["latitude"] = child[2]
            child1_child_query = ("SELECT child_id_1, child_id_2, driving_distance, driving_time "
                                  "FROM locationdata "
                                  f"WHERE child_id_1 = '{child[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
            cursor.execute(child1_child_query)
            child1_child = cursor.fetchall()

            for row2 in child1_child:
                # Add columns with prefixes
                data[f'{row2[1]}_distance'] = row2[2]
                data[f'{row2[1]}_time'] = row2[3]

            child_child2_query = ("SELECT child_id_1, child_id_2, driving_distance, driving_time "
                                  "FROM locationdata "
                                  f"WHERE child_id_2 = '{child[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
            cursor.execute(child_child2_query)
            child_child2 = cursor.fetchall()

            for row3 in child_child2:
                data[f'{row3[0]}_distance'] = row3[2]
                data[f'{row3[0]}_time'] = row3[3]

            child_ei_query = ("SELECT child_id_1, ei_id_1, driving_distance, driving_time "
                              "FROM locationdata "
                              f"WHERE child_id_1 = '{child[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
            cursor.execute(child_ei_query)
            child_ei = cursor.fetchall()

            for row4 in child_ei:
                data[f'{row4[1]}_distance'] = row4[2]
                data[f'{row4[1]}_time'] = row4[3]

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
        new_row["assignment_count"] = 1
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

    query = ("Select ei_id, longitude, latitude from ei "
             "WHERE ei_id != '65d75e28e09e67e025e8cd3c'")
    cursor.execute(query)

    ei_ids = cursor.fetchall()

    eiDFs = []

    for ei in ei_ids:
        ei_group_list = []
        data = {}
        data["id"] = ei[0]
        data["longitude"] = ei[1]
        data["latitude"] = ei[2]
        ei_child_query = ("SELECT child_id_1, ei_id_1, driving_distance, driving_time "
                          "FROM locationdata "
                          f"WHERE ei_id_1 = '{ei[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(ei_child_query)
        ei_child = cursor.fetchall()

        for row2 in ei_child:
            # Add columns with prefixes
            data[f'{row2[0]}_distance'] = row2[2]
            data[f'{row2[0]}_time'] = row2[3]

        ei1_ei_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time "
                        "FROM locationdata "
                        f"WHERE ei_id_1 = '{ei[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei1_ei_query)
        ei1_ei = cursor.fetchall()

        for row3 in ei1_ei:
            data[f'{row3[1]}_distance'] = row3[2]
            data[f'{row3[1]}_time'] = row3[3]

        ei_ei2_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time "
                        "FROM locationdata "
                        f"WHERE ei_id_2 = '{ei[0]}' AND child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei_ei2_query)
        ei_ei2 = cursor.fetchall()

        for row4 in ei_ei2:
            data[f'{row4[0]}_distance'] = row4[2]
            data[f'{row4[0]}_time'] = row4[3]

        ei_group_list.append(data)

        childQuery = ("SELECT child.child_id, child.longitude, child.latitude FROM locationdatareal ld "
                      "join child on child.child_id = ld.child_id_1 "
                      f"where child_id_2 is null and ei_id_2 is null and ld.assigned = 1 and ei_id_1 = '{ei[0]}'")

        cursor.execute(childQuery)
        assignedChild = cursor.fetchall()

        for child in assignedChild:
            data = {}
            data["id"] = child[0]
            data["longitude"] = child[1]
            data["latitude"] = child[2]
            child1_child_query = ("SELECT child_id_1, child_id_2, driving_distance, driving_time "
                                  "FROM locationdata "
                                  f"WHERE child_id_1 = '{child[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
            cursor.execute(child1_child_query)
            child1_child = cursor.fetchall()

            for row2 in child1_child:
                # Add columns with prefixes
                data[f'{row2[1]}_distance'] = row2[2]
                data[f'{row2[1]}_time'] = row2[3]

            child_child2_query = ("SELECT child_id_1, child_id_2, driving_distance, driving_time "
                                  "FROM locationdata "
                                  f"WHERE child_id_2 = '{child[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
            cursor.execute(child_child2_query)
            child_child2 = cursor.fetchall()

            for row3 in child_child2:
                data[f'{row3[0]}_distance'] = row3[2]
                data[f'{row3[0]}_time'] = row3[3]

            child_ei_query = ("SELECT child_id_1, ei_id_1, driving_distance, driving_time "
                              "FROM locationdata "
                              f"WHERE child_id_1 = '{child[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
            cursor.execute(child_ei_query)
            child_ei = cursor.fetchall()

            for row4 in child_ei:
                data[f'{row4[1]}_distance'] = row4[2]
                data[f'{row4[1]}_time'] = row4[3]

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