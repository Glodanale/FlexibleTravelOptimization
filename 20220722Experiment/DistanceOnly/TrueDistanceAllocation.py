import pandas as pd
import numpy as np
import mysql.connector

def distanceNoCap(table):
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expanding_horizons",
                password = "dB79*dG2024!"
    )

    cursor = connection.cursor()

    query = ("UPDATE child "
             "SET assigned = 0")
    cursor.execute(query)
    connection.commit()

    query = ("UPDATE ei "
             "SET Allo_Count = 0")
    cursor.execute(query)
    connection.commit()

    query = (f"UPDATE {table} "
             "SET assigned = 0")
    cursor.execute(query)
    connection.commit()

    query = ("SELECT child_id from child")
    cursor.execute(query)

    child_ids = cursor.fetchall()

    for child in child_ids:
        query = ("Select ld.child_id_1, ld.ei_id_1, ld.assigned, ld.driving_distance, ld.driving_time, child.Longitude, child.Latitude, child.assigned, ei.Allo_Count "
                 "from locationdata ld "
                 "join child on child.child_id=ld.child_id_1 "
                 "join ei on ei.ei_id = ld.ei_id_1 "
                 f"where ld.child_id_2 is null and ld.ei_id_2 is null and child.assigned = 0 and ld.child_id_1 = '{child[0]}' and ld.ei_id_1 != '65d75e28e09e67e025e8cd3c'"
                 "order by driving_distance asc")
        cursor.execute(query)
        child_data = cursor.fetchall()

        if child_data:
            for child in child_data:
                ei = child[1]
                longitude = child[5]
                latitude = child[6]
                alloCount = child[8]
                break
            alloCount += 1

            updateQuery = (f"UPDATE {table} ld "
                           "JOIN child on ld.child_id_1 = child.child_id "
                           "JOIN ei on ei.ei_id = ld.ei_id_1 "
                           f"SET ld.assigned = 1, child.assigned = 1, ei.Allo_Count = {alloCount} " 
                           f"WHERE child.longitude = {longitude} and child.latitude = {latitude} and ld.ei_id_1 = '{ei}'")
            cursor.execute(updateQuery)
            connection.commit()
            #print("Update query pushed")
        else:
            query = ("Select ld.child_id_1, ld.ei_id_1, ld.assigned, ld.driving_distance, ld.driving_time, child.Longitude, child.Latitude, child.assigned "
                     f"from {table} ld "
                     "join child on child.child_id=ld.child_id_1 "
                     f"where ld.child_id_2 is null and ld.ei_id_2 is null and child.assigned = 1 and ld.child_id_1 = '{child[0]}' "
                     "order by driving_distance asc")
            cursor.execute(query)
            data = cursor.fetchall()
            
            '''
            if data:
                print(f"{child[0]} already assigned")
            else:
                print("Error")
            '''

    cursor.close()
    connection.close()

def distanceUniformCap(child_ids, table):    
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expanding_horizons",
                password = "dB79*dG2024!"
    )

    cursor = connection.cursor()

    query = ("UPDATE child "
             "SET assigned = 0")
    cursor.execute(query)
    connection.commit()

    query = ("UPDATE ei "
             "SET Allo_Count = 0")
    cursor.execute(query)
    connection.commit()

    query = (f"UPDATE {table} "
             "SET assigned = 0")
    cursor.execute(query)
    connection.commit()

    for index, row in child_ids.iterrows():
        id_value = row["id"]
        query = (f"Select ld.child_id_1, ld.ei_id_1, ld.assigned, ld.driving_distance, ld.driving_time, child.Longitude, child.Latitude, child.assigned, ei.Allo_Count from {table} ld "
                 "join child on child.child_id=ld.child_id_1 "
                 "join ei on ei.ei_id = ld.ei_id_1 "
                 f"where ld.child_id_2 is null and ld.ei_id_2 is null and child.assigned = 0 and ld.child_id_1 = '{id_value}' and ld.ei_id_1 != '65d75e28e09e67e025e8cd3c' and ei.Allo_Count < 17 "
                 "order by driving_distance asc")
        cursor.execute(query)
        child_data = cursor.fetchall()

        if child_data:
            for child in child_data:
                ei = child[1]
                longitude = child[5]
                latitude = child[6]
                alloCount = child[8]
                break
            alloCount += 1

            updateQuery = (f"UPDATE {table} ld "
                           "JOIN child on ld.child_id_1 = child.child_id "
                           "JOIN ei on ei.ei_id = ld.ei_id_1 "
                           f"SET ld.assigned = 1, child.assigned = 1, ei.Allo_Count = {alloCount} " 
                           f"WHERE child.longitude = {longitude} and child.latitude = {latitude} and ld.ei_id_1 = '{ei}'")
            cursor.execute(updateQuery)
            connection.commit()
            #print("Update query pushed")
        else:
            query = (f"Select ld.child_id_1, ld.ei_id_1, ld.assigned, ld.driving_distance, ld.driving_time, child.Longitude, child.Latitude, child.assigned from {table} ld "
                     "join child on child.child_id=ld.child_id_1 "
                     f"where ld.child_id_2 is null and ld.ei_id_2 is null and child.assigned = 1 and ld.child_id_1 = '{id_value}' "
                     "order by driving_distance asc")
            cursor.execute(query)
            data = cursor.fetchall()

            '''
            if data:
                print(f"{id_value} already assigned")
            else:
                print("Error")
            '''

    cursor.close()
    connection.close()

def distanceRealCap(child_ids, table):    
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expanding_horizons",
                password = "dB79*dG2024!"
    )

    cursor = connection.cursor()

    query = ("UPDATE child "
             "SET assigned = 0")
    cursor.execute(query)
    connection.commit()

    query = ("UPDATE ei "
             "SET Allo_Count = 0")
    cursor.execute(query)
    connection.commit()

    query = (f"UPDATE {table} "
             "SET assigned = 0")
    cursor.execute(query)
    connection.commit()

    for index, row in child_ids.iterrows():
        id_value = row["id"]
        query = ("with eiCounts as ( "
                 "Select ei_id_1, count(assigned) Count_Assigned "
                 "from locationdatareal "
                 "where assigned = 1 "
                 "group by ei_id_1"
                 ") "
                 "Select ld.child_id_1, ld.ei_id_1, ld.assigned, ld.driving_distance, ld.driving_time, child.Longitude, child.Latitude, child.assigned, ei.Allo_Count, eiCounts.Count_Assigned "
                 "from locationdata ld "
                 "join child on child.child_id=ld.child_id_1 "
                 "join ei on ei.ei_id = ld.ei_id_1 "
                 "join eiCounts on ei.ei_id = eiCounts.ei_id_1 "
                 f"where ld.child_id_2 is null and ld.ei_id_2 is null and child.assigned = 0 and ld.child_id_1 = '{id_value}' and ld.ei_id_1 != '65d75e28e09e67e025e8cd3c' and ei.Allo_Count < eiCounts.Count_Assigned "
                 "order by driving_distance asc")
        cursor.execute(query)
        child_data = cursor.fetchall()

        if child_data:
            for child in child_data:
                ei = child[1]
                longitude = child[5]
                latitude = child[6]
                alloCount = child[8]
                break
            alloCount += 1

            updateQuery = (f"UPDATE {table} ld "
                           "JOIN child on ld.child_id_1 = child.child_id "
                           "JOIN ei on ei.ei_id = ld.ei_id_1 "
                           f"SET ld.assigned = 1, child.assigned = 1, ei.Allo_Count = {alloCount} " 
                           f"WHERE child.longitude = {longitude} and child.latitude = {latitude} and ld.ei_id_1 = '{ei}'")
            cursor.execute(updateQuery)
            connection.commit()
            #print("Update query pushed")
        else:
            query = (f"Select ld.child_id_1, ld.ei_id_1, ld.assigned, ld.driving_distance, ld.driving_time, child.Longitude, child.Latitude, child.assigned from {table} ld "
                     "join child on child.child_id=ld.child_id_1 "
                     f"where ld.child_id_2 is null and ld.ei_id_2 is null and child.assigned = 1 and ld.child_id_1 = '{id_value}' "
                     "order by driving_distance asc")
            cursor.execute(query)
            data = cursor.fetchall()

            '''
            if data:
                print(f"{id_value} already assigned")
            else:
                print("Error")
            '''

    cursor.close()
    connection.close()