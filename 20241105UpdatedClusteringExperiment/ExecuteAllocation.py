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




def sumClustersUniform(eiID, childRow, clusterDF):
    clusterDFCopy = clusterDF.copy()
    eiRow = clusterDFCopy.loc[clusterDFCopy['id'] == eiID].iloc[0]  # Select the EI row
    assignment_count = eiRow["assignment_count"]

    # Compute weighted average
    updated_values = (eiRow.drop(['id', 'assignment_count']) * assignment_count + childRow.drop(['id', 'assigned'])) / (assignment_count + 1)
    updated_values["id"] = eiID
    updated_values["assignment_count"] = assignment_count + 1

    # Update the cluster DataFrame
    clusterDFCopy.update(pd.DataFrame([updated_values]))
    
    return clusterDFCopy




def sumClustersWeighted(eiID, childRow, clusterDF, originalEIData):
    clusterDFCopy = clusterDF.copy()
    originalCopy = originalEIData.copy()
    eiRow = originalCopy.loc[originalCopy['id'] == eiID].iloc[0]
    eiClusterRow = clusterDFCopy.loc[clusterDFCopy['id'] == eiID].iloc[0]  # Select the EI row
    assignment_count = eiClusterRow["assignment_count"]

    merge_values = (eiRow.drop(['id', 'assignment_count']) + childRow.drop(['id', 'assigned'])) / 2

    # Compute weighted average
    updated_values = (eiClusterRow.drop(['id', 'assignment_count']) * assignment_count + merge_values) / (assignment_count + 1)
    updated_values["id"] = eiID
    updated_values["assignment_count"] = assignment_count + 1

    # Update the cluster DataFrame
    clusterDFCopy.update(pd.DataFrame([updated_values]))
    
    return clusterDFCopy



def uniformAllocationNoCap(childDataFrameOrdered, eiDataFrameOrdered, table):
    childDataFrameAllocate = childDataFrameOrdered.copy()
    eiDFsums = eiDataFrameOrdered.copy()
    
    for child_index, child_row in childDataFrameAllocate.iterrows():
        if child_row["assigned"] == "":
            # Calculate distances to all EIs
            distances = eiDataFrameOrdered.apply(lambda ei_row: euclidean_distance(child_row, ei_row), axis=1)
            min_index = distances.idxmin()
            assigned_ei = eiDataFrameOrdered.loc[min_index, 'id']

            # Assign child and same-location children
            same_location_children = childDataFrameAllocate[
                (childDataFrameAllocate["longitude"] == child_row["longitude"]) &
                (childDataFrameAllocate["latitude"] == child_row["latitude"])
            ]
            childDataFrameAllocate.loc[same_location_children.index, 'assigned'] = assigned_ei

            # Update EI with new children
            for _, same_child_row in same_location_children.iterrows():
                eiDFsums = sumClustersUniform(assigned_ei, same_child_row, eiDFsums)

    # Print final cluster assignments
    for _, row in eiDFsums.iterrows():
        print(f"EI id: {row['id']} - Count: {row['assignment_count']}")

    pushAllocation(childDataFrameAllocate, table)
    return childDataFrameAllocate




def weightedAllocationNoCap(childDataFrameOrdered, eiDataFrameOrdered, table):
    # Keep a reference to the original EI data
    originalEIData = eiDataFrameOrdered.copy()
    
    childDataFrameAllocate = childDataFrameOrdered.copy()
    eiDFsums = eiDataFrameOrdered.copy()
    
    for child_index, child_row in childDataFrameAllocate.iterrows():
        if child_row["assigned"] == "":
            # Calculate distances to all EIs
            distances = eiDataFrameOrdered.apply(lambda ei_row: euclidean_distance(child_row, ei_row), axis=1)
            min_index = distances.idxmin()
            assigned_ei = eiDataFrameOrdered.loc[min_index, 'id']

            # Assign child and same-location children
            same_location_children = childDataFrameAllocate[
                (childDataFrameAllocate["longitude"] == child_row["longitude"]) &
                (childDataFrameAllocate["latitude"] == child_row["latitude"])
            ]
            childDataFrameAllocate.loc[same_location_children.index, 'assigned'] = assigned_ei

            # Update EI with new children
            for _, same_child_row in same_location_children.iterrows():
                eiDFsums = sumClustersWeighted(assigned_ei, same_child_row, eiDFsums, originalEIData)

    # Print final cluster assignments
    for _, row in eiDFsums.iterrows():
        print(f"EI id: {row['id']} - Count: {row['assignment_count']}")

    pushAllocation(childDataFrameAllocate, table)
    return childDataFrameAllocate





def uniformAllocationUniform(childDataFrameOrdered, eiDataFrameOrdered, table):
    # Set a fixed capacity limit for all EIs
    capacity_limit = 18
    
    # Initialize current assignment counts for EIs
    eiCurrentAssignments = {ei_id: 0 for ei_id in eiDataFrameOrdered["id"].tolist()}

    childDataFrameAllocate = childDataFrameOrdered.copy()
    eiDFsums = eiDataFrameOrdered.copy()

    for child_index, child_row in childDataFrameAllocate.iterrows():
        if child_row["assigned"] == "":
            # Filter eligible EIs based on the fixed capacity limit
            eligibleEIs = eiDataFrameOrdered[eiDataFrameOrdered["id"].isin(
                [ei_id for ei_id, current_count in eiCurrentAssignments.items() if current_count < capacity_limit]
            )]

            if eligibleEIs.empty:
                print("No eligible EIs left with available capacity.")
                break  # No EIs available for assignment

            # Calculate distances to eligible EIs
            distances = eligibleEIs.apply(lambda ei_row: euclidean_distance(child_row, ei_row), axis=1)
            min_index = distances.idxmin()
            assigned_ei = eligibleEIs.loc[min_index, 'id']

            # Assign child and same-location children
            same_location_children = childDataFrameAllocate[
                (childDataFrameAllocate["longitude"] == child_row["longitude"]) &
                (childDataFrameAllocate["latitude"] == child_row["latitude"])
            ]
            childDataFrameAllocate.loc[same_location_children.index, 'assigned'] = assigned_ei

            # Update EI with new children
            for _, same_child_row in same_location_children.iterrows():
                eiDFsums = sumClustersUniform(assigned_ei, same_child_row, eiDFsums)

            # Update the current assignment count for the EI
            eiCurrentAssignments[assigned_ei] += len(same_location_children)

    # Print final cluster assignments
    for ei_id, count in eiCurrentAssignments.items():
        print(f"EI id: {ei_id} - Assigned Count: {count}")

    pushAllocation(childDataFrameAllocate, table)
    return childDataFrameAllocate




def weightedAllocationUniform(childDataFrameOrdered, eiDataFrameOrdered, table):
    # Set a fixed capacity limit for all EIs
    capacity_limit = 18
    
    # Initialize current assignment counts for EIs
    eiCurrentAssignments = {ei_id: 0 for ei_id in eiDataFrameOrdered["id"].tolist()}

    # Keep a reference to the original EI data
    originalEIData = eiDataFrameOrdered.copy()

    childDataFrameAllocate = childDataFrameOrdered.copy()
    eiDFsums = eiDataFrameOrdered.copy()

    for child_index, child_row in childDataFrameAllocate.iterrows():
        if child_row["assigned"] == "":
            # Filter eligible EIs based on the fixed capacity limit
            eligibleEIs = eiDataFrameOrdered[eiDataFrameOrdered["id"].isin(
                [ei_id for ei_id, current_count in eiCurrentAssignments.items() if current_count < capacity_limit]
            )]

            if eligibleEIs.empty:
                print("No eligible EIs left with available capacity.")
                break  # No EIs available for assignment

            # Calculate distances to eligible EIs
            distances = eligibleEIs.apply(lambda ei_row: euclidean_distance(child_row, ei_row), axis=1)
            min_index = distances.idxmin()
            assigned_ei = eligibleEIs.loc[min_index, 'id']

            # Assign child and same-location children
            same_location_children = childDataFrameAllocate[
                (childDataFrameAllocate["longitude"] == child_row["longitude"]) &
                (childDataFrameAllocate["latitude"] == child_row["latitude"])
            ]
            childDataFrameAllocate.loc[same_location_children.index, 'assigned'] = assigned_ei

            # Update EI with new children
            for _, same_child_row in same_location_children.iterrows():
                eiDFsums = sumClustersWeighted(assigned_ei, same_child_row, eiDFsums, originalEIData)

            # Update the current assignment count for the EI
            eiCurrentAssignments[assigned_ei] += len(same_location_children)

    # Print final cluster assignments
    for ei_id, count in eiCurrentAssignments.items():
        print(f"EI id: {ei_id} - Assigned Count: {count}")

    pushAllocation(childDataFrameAllocate, table)
    return childDataFrameAllocate





def uniformAllocationRealCap(childDataFrameOrdered, eiDataFrameOrdered, table):
    # Get EI capacity limits
    eiCapacityDF = clusterSets.eiAllocationQuantity()
    eiCapacityDF.set_index('ei_id', inplace=True)

    # Initialize current assignment counts for EIs
    eiCurrentAssignments = eiCapacityDF["Count_Assigned"].to_dict()

    childDataFrameAllocate = childDataFrameOrdered.copy()
    eiDFsums = eiDataFrameOrdered.copy()

    for child_index, child_row in childDataFrameAllocate.iterrows():
        if child_row["assigned"] == "":
            # Filter eligible EIs based on capacity
            eligibleEIs = eiDataFrameOrdered[eiDataFrameOrdered["id"].isin(
                [ei_id for ei_id, current_count in eiCurrentAssignments.items() if current_count < eiCapacityDF.loc[ei_id, "Count_Assigned"]]
            )]

            if eligibleEIs.empty:
                print("No eligible EIs left with available capacity.")
                break  # No EIs available for assignment

            # Calculate distances to eligible EIs
            distances = eligibleEIs.apply(lambda ei_row: euclidean_distance(child_row, ei_row), axis=1)
            min_index = distances.idxmin()
            assigned_ei = eligibleEIs.loc[min_index, 'id']

            # Assign child and same-location children
            same_location_children = childDataFrameAllocate[
                (childDataFrameAllocate["longitude"] == child_row["longitude"]) &
                (childDataFrameAllocate["latitude"] == child_row["latitude"])
            ]
            childDataFrameAllocate.loc[same_location_children.index, 'assigned'] = assigned_ei

            # Update EI with new children
            for _, same_child_row in same_location_children.iterrows():
                eiDFsums = sumClustersUniform(assigned_ei, same_child_row, eiDFsums)

            # Update the current assignment count for the EI
            eiCurrentAssignments[assigned_ei] += len(same_location_children)

    # Print final cluster assignments
    for _, row in eiDFsums.iterrows():
        print(f"EI id: {row['id']} - Count: {row['assignment_count']}")

    pushAllocation(childDataFrameAllocate, table)
    return childDataFrameAllocate





def weightedAllocationRealCap(childDataFrameOrdered, eiDataFrameOrdered, table):
    # Get EI capacity limits
    eiCapacityDF = clusterSets.eiAllocationQuantity()
    eiCapacityDF.set_index('ei_id', inplace=True)

    # Initialize current assignment counts for EIs
    eiCurrentAssignments = eiCapacityDF["Count_Assigned"].to_dict()

    # Keep a reference to the original EI data
    originalEIData = eiDataFrameOrdered.copy()

    childDataFrameAllocate = childDataFrameOrdered.copy()
    eiDFsums = eiDataFrameOrdered.copy()

    for child_index, child_row in childDataFrameAllocate.iterrows():
        if child_row["assigned"] == "":
            # Filter eligible EIs based on capacity
            eligibleEIs = eiDataFrameOrdered[eiDataFrameOrdered["id"].isin(
                [ei_id for ei_id, current_count in eiCurrentAssignments.items() if current_count < eiCapacityDF.loc[ei_id, "Count_Assigned"]]
            )]

            if eligibleEIs.empty:
                print("No eligible EIs left with available capacity.")
                break  # No EIs available for assignment

            # Calculate distances to eligible EIs
            distances = eligibleEIs.apply(lambda ei_row: euclidean_distance(child_row, ei_row), axis=1)
            min_index = distances.idxmin()
            assigned_ei = eligibleEIs.loc[min_index, 'id']

            # Assign child and same-location children
            same_location_children = childDataFrameAllocate[
                (childDataFrameAllocate["longitude"] == child_row["longitude"]) &
                (childDataFrameAllocate["latitude"] == child_row["latitude"])
            ]
            childDataFrameAllocate.loc[same_location_children.index, 'assigned'] = assigned_ei

            # Update EI with new children
            for _, same_child_row in same_location_children.iterrows():
                eiDFsums = sumClustersWeighted(assigned_ei, same_child_row, eiDFsums, originalEIData)

            # Update the current assignment count for the EI
            eiCurrentAssignments[assigned_ei] += len(same_location_children)

    # Print final cluster assignments
    for _, row in eiDFsums.iterrows():
        print(f"EI id: {row['id']} - Count: {row['assignment_count']}")

    pushAllocation(childDataFrameAllocate, table)
    return childDataFrameAllocate






def pushAllocation(childDF, table, reset=False):
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expanding_horizons",
                password = "dB79*dG2024!"
    )

    cursor = connection.cursor()
    
    if (reset == True):
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