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
        data["longitude"] = row[1]
        data["latitude"] = row[2]

        ei_list.append(data)

    eiDataFrame = pd.DataFrame(ei_list)
    eiDataFrame = eiDataFrame[eiDataFrame['id'] != '65d75e28e09e67e025e8cd3c']  #drop this EI because she is currently not seeing children
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