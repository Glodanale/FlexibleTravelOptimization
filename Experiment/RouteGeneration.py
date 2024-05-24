import pandas as pd
import numpy as np
import random
import mysql.connector

def generatePermutations(eiID, childList, clusterDF):
    routeSums = []
    for i in range(0, 1000):
        random.shuffle(childList)
        clusterList = [eiID] + childList + [eiID]
        clusterRoute = 0
        for j in range(0, len(clusterList)-1):
            distance_value = clusterDF.loc[(clusterDF["Location_1"] == clusterList[j]) & (clusterDF["Location_2"] == clusterList[j+1]), "Distance"].values[0]
            clusterRoute += distance_value
        routeSums.append(clusterRoute)
    routeAverage = sum(routeSums) / len(routeSums)
    
    return routeAverage

def calculateDistances(table):
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expandinghorizons",
                password = "dB79@dG2024!"
    )

    cursor = connection.cursor()
    
    query = ("SELECT ei_id from ei "
             "WHERE ei_id != '65d75e28e09e67e025e8cd3c'")
    
    cursor.execute(query)
    ei_ids = cursor.fetchall()
    
    eiClusterAverages = pd.DataFrame(columns=["ei_id", "Route_Distance", "ClusterQuant", "RouteOverQuant"])
    for ei in ei_ids: 
        eiClusterDistances = pd.DataFrame(columns=["Location_1", "Location_2", "Distance"])
        
        query = ("SELECT child_id_1, ei_id_1, driving_distance "
                 f"FROM {table} "
                 f"WHERE ei_id_1 = '{ei[0]}' and child_id_2 is null and ei_id_2 is null and assigned = 1")
        
        cursor.execute(query)
        childrenToEI = cursor.fetchall()
        CE = pd.DataFrame(childrenToEI, columns=["Location_1", "Location_2", "Distance"])
        eiClusterDistances = pd.concat([eiClusterDistances, CE], ignore_index=True)
        
        query = ("SELECT ei_id_1, child_id_1, driving_distance "
                 f"FROM {table} "
                 f"WHERE ei_id_1 = '{ei[0]}' and child_id_2 is null and ei_id_2 is null and assigned = 1")
        
        cursor.execute(query)
        EItoChildren = cursor.fetchall()
        CE = pd.DataFrame(EItoChildren, columns=["Location_1", "Location_2", "Distance"])
        eiClusterDistances = pd.concat([eiClusterDistances, CE], ignore_index=True)
        
        child_list = []
        
        for child1 in EItoChildren:
            child_list.append(child1[1])
            for child2 in EItoChildren:
                if child1[1] != child2[1]:
                    query = ("SELECT child_id_1, child_id_2, driving_distance "
                             f"FROM {table} "
                             f"WHERE (child_id_1 = '{child1[1]}' and child_id_2 = '{child2[1]}')")
                    cursor.execute(query)
                    childToChild = cursor.fetchall()
                    if childToChild:
                        CC = pd.DataFrame(childToChild, columns=["Location_1", "Location_2", "Distance"])
                        eiClusterDistances = pd.concat([eiClusterDistances, CC], ignore_index=True)
                        
                        query = ("SELECT child_id_2, child_id_1, driving_distance "
                                 f"FROM {table} "
                                 f"WHERE (child_id_1 = '{child1[1]}' and child_id_2 = '{child2[1]}')")
                        cursor.execute(query)
                        childToChild2 = cursor.fetchall()
                        CC = pd.DataFrame(childToChild2, columns=["Location_1", "Location_2", "Distance"])
                        eiClusterDistances = pd.concat([eiClusterDistances, CC], ignore_index=True)
        
        routeAverage = generatePermutations(ei[0], child_list, eiClusterDistances)
        quant = len(child_list) + 1
        routeOverQuant = routeAverage / quant
        result = {"ei_id": ei[0], "Route_Distance": routeAverage, "ClusterQuant": quant, "RouteOverQuant": routeOverQuant}
        resultDF = pd.DataFrame([result])
        eiClusterAverages = pd.concat([eiClusterAverages, resultDF], ignore_index=True)
    min_distance = eiClusterAverages["RouteOverQuant"].min()
    average_distance = eiClusterAverages["RouteOverQuant"].mean()
    max_distance = eiClusterAverages["RouteOverQuant"].max()
    variance_distance = eiClusterAverages["RouteOverQuant"].var()
    
    return min_distance, average_distance, max_distance, variance_distance

