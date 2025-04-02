import pandas as pd
import numpy as np
import mysql.connector
import clusterSets

def euclidean_distance(childRow, eiRow):
    childRowCopy = childRow.copy()
    eiRowCopy = eiRow.copy()
    child_values = childRowCopy.drop(['id', 'assigned']).values
    ei_values = eiRowCopy.drop(['id', 'assignment_count']).values
    return np.sqrt(np.sum((child_values - ei_values) ** 2))

def allocationNoCap(childDataFrameOrdered, eiDataFrameOrdered, table):
    childDataFrameAllocate1 = childDataFrameOrdered.copy()
    childDataFrameAllocate2 = childDataFrameOrdered.copy()
    eiDFset = eiDataFrameOrdered.copy()
    for child_index, child_row in childDataFrameAllocate1.iterrows():
        if childDataFrameAllocate2.loc[child_index, "assigned"] == "":
            availableEIs = eiDFset
            eiDF = []
            for ei_index, ei_row in availableEIs.iterrows():
                e_value = euclidean_distance(child_row, ei_row)
                #eiDF = pd.concat([eiDF, pd.DataFrame({"id": [ei_row["id"]], "euclidean_distance": [e_value]})], ignore_index=True)
                ei_row = {"id": [ei_row["id"]], "euclidean_distance": [e_value]}
                eiDF.append(ei_row)
            eiDF = pd.DataFrame(eiDF)
            minIndex = eiDF["euclidean_distance"].idxmin()
            ei_with_min = eiDF.at[minIndex, "id"]
            ei_with_min = ei_with_min[0]

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
        
    for index, row in eiDFset.iterrows():
        print(f"EI id: {row['id']}               count: {row['assignment_count']}")

    pushAllocation(childDataFrameAllocate1, table)

    return childDataFrameAllocate1

def allocationUniform(childDataFrameOrdered, eiDataFrameOrdered, table):
    childDataFrameAllocate1 = childDataFrameOrdered.copy()
    childDataFrameAllocate2 = childDataFrameOrdered.copy()
    eiDFset = eiDataFrameOrdered.copy()
    for child_index, child_row in childDataFrameAllocate1.iterrows():
        if childDataFrameAllocate2.loc[child_index, "assigned"] == "":
            availableEIs = eiDFset[eiDFset['assignment_count'] < 17]
            eiDF = []
            for ei_index, ei_row in availableEIs.iterrows():
                e_value = euclidean_distance(child_row, ei_row)
                #eiDF = pd.concat([eiDF, pd.DataFrame({"id": [ei_row["id"]], "euclidean_distance": [e_value]})], ignore_index=True)
                ei_row = {"id": [ei_row["id"]], "euclidean_distance": [e_value]}
                eiDF.append(ei_row)
            eiDF = pd.DataFrame(eiDF)
            minIndex = eiDF["euclidean_distance"].idxmin()
            ei_with_min = eiDF.at[minIndex, "id"]
            ei_with_min = ei_with_min[0]

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

    pushAllocation(childDataFrameAllocate1, table)

    return childDataFrameAllocate1


def allocationRealCap(childDataFrameOrdered, eiDataFrameOrdered, table):
    childDataFrameAllocate1 = childDataFrameOrdered.copy()
    childDataFrameAllocate2 = childDataFrameOrdered.copy()
    eiDFset = eiDataFrameOrdered.copy()
    eiCounterDF = clusterSets.eiAllocationQuantity()
    for child_index, child_row in childDataFrameAllocate1.iterrows():
        if childDataFrameAllocate2.loc[child_index, "assigned"] == "":
            ei_counter_dict = eiCounterDF.set_index('ei_id')['Count_Assigned'].to_dict()
            availableEIs = eiDFset[eiDFset.apply(lambda row: ei_counter_dict.get(row['id'], 0) > row['assignment_count'], axis=1)]
            eiDF = []
            for ei_index, ei_row in availableEIs.iterrows():
                e_value = euclidean_distance(child_row, ei_row)
                #eiDF = pd.concat([eiDF, pd.DataFrame({"id": [ei_row["id"]], "euclidean_distance": [e_value]})], ignore_index=True)
                ei_row = {"id": [ei_row["id"]], "euclidean_distance": [e_value]}
                eiDF.append(ei_row)
            eiDF = pd.DataFrame(eiDF)
            minValue = eiDF["euclidean_distance"].min()
            minIndex = eiDF["euclidean_distance"].idxmin()
            ei_with_min = eiDF.at[minIndex, "id"]
            ei_with_min = ei_with_min[0]

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
        
    df = eiDFset[["id", "assignment_count"]]
    print(df)

    pushAllocation(childDataFrameAllocate1, table)

    return childDataFrameAllocate1


def pushAllocation(childDF, table):
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