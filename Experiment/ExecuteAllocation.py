import pandas as pd
import numpy as np
import mysql.connector

def euclidean_distance(childRow, eiRow):
    child_values = childRow.drop(['id', 'assigned']).values
    ei_values = eiRow.drop(['id', 'assignment_count']).values
    return np.sqrt(np.sum((child_values - ei_values) ** 2))

def manhattan_distance(childRow, eiRow):
    child_values = childRow.drop(['id', 'assigned']).values
    ei_values = eiRow.drop(['id', 'assignment_count']).values
    return np.sum(np.abs(child_values - ei_values))

def allocationEuclidean(childDataFrameOrdered, eiDataFrameOrdered):
    childDataFrameAllocate1 = childDataFrameOrdered.copy()
    childDataFrameAllocate2 = childDataFrameOrdered.copy()
    eiDFset = eiDataFrameOrdered.copy()
    for child_index, child_row in childDataFrameAllocate1.iterrows():
        if childDataFrameAllocate2.loc[child_index, "assigned"] == "":
            availableEIs = eiDFset[eiDFset['assignment_count'] < 17]
            eiDF = pd.DataFrame(columns=["id", "euclidean_distance"])
            for ei_index, ei_row in availableEIs.iterrows():
                e_value = euclidean_distance(child_row, ei_row)
                eiDF = pd.concat([eiDF, pd.DataFrame({"id": [ei_row["id"]], "euclidean_distance": [e_value]})], ignore_index=True)
            minValue = eiDF["euclidean_distance"].min()
            minIndex = eiDF["euclidean_distance"].idxmin()
            ei_with_min = eiDF.at[minIndex, "id"]

            same_location_children = childDataFrameAllocate1[(childDataFrameAllocate1["longitude"] == child_row["longitude"]) & 
                                                          (childDataFrameAllocate1["latitude"] == child_row["latitude"])]
            childDataFrameAllocate1.loc[same_location_children.index, 'assigned'] = ei_with_min

            rows_with_ei_with_min = childDataFrameAllocate1[childDataFrameAllocate1['assigned'] == ei_with_min]
            count_ei_with_min = len(rows_with_ei_with_min)

            ei_index = eiDFset[eiDFset['id'] == ei_with_min].index
            eiDFset.at[ei_index[0], 'assignment_count'] = count_ei_with_min
            #print(f"ChildID: {child_row['id']}     AssignedEI: {ei_with_min}")
        #else:
            #print("child already assigned")
        childDataFrameAllocate2 = childDataFrameAllocate1.copy()

    return childDataFrameAllocate1


def allocationManhattan(childDF, clusterDF):
    childDataFrameAllocate1 = childDF.copy()
    childDataFrameAllocate2 = childDF.copy()
    eiDFset = clusterDF.copy()
    for child_index, child_row in childDataFrameAllocate1.iterrows():
        if childDataFrameAllocate2.loc[child_index, "assigned"] == "":
            availableEIs = eiDFset[eiDFset['assignment_count'] < 17]
            eiDF = pd.DataFrame(columns=["id", "manhattan_distance"])
            for ei_index, ei_row in availableEIs.iterrows():
                e_value = manhattan_distance(child_row, ei_row)
                eiDF = pd.concat([eiDF, pd.DataFrame({"id": [ei_row["id"]], "manhattan_distance": [e_value]})], ignore_index=True)
            minValue = eiDF["manhattan_distance"].min()
            minIndex = eiDF["manhattan_distance"].idxmin()
            ei_with_min = eiDF.at[minIndex, "id"]

            same_location_children = childDataFrameAllocate1[(childDataFrameAllocate1["longitude"] == child_row["longitude"]) & 
                                                          (childDataFrameAllocate1["latitude"] == child_row["latitude"])]
            childDataFrameAllocate1.loc[same_location_children.index, 'assigned'] = ei_with_min

            rows_with_ei_with_min = childDataFrameAllocate1[childDataFrameAllocate1['assigned'] == ei_with_min]
            count_ei_with_min = len(rows_with_ei_with_min)

            ei_index = eiDFset[eiDFset['id'] == ei_with_min].index
            eiDFset.at[ei_index[0], 'assignment_count'] = count_ei_with_min
        #else:
            #print("child already assigned")
        childDataFrameAllocate2 = childDataFrameAllocate1.copy()

    return childDataFrameAllocate1


def pushAllocation(childDF, table):
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expandinghorizons",
                password = "dB79@dG2024!"
    )

    cursor = connection.cursor()
    
    query = (f"UPDATE {table} "
             "SET assigned = 0")
    cursor.execute(query)
    connection.commit()
    
    for index, child in childDF.iterrows():
        child_id = child["id"]
        ei_id = child["assigned"]
        updateQuery = (f"Update {table} "
                       "SET assigned = 1 "
                       f"WHERE child_id_1 = '{child_id}' and ei_id_1 = '{ei_id}' and child_id_2 is NULL and ei_id_2 is NULL")
        cursor.execute(updateQuery)
        connection.commit()
        #print("Update query pushed")
        
    cursor.close()
    connection.close()