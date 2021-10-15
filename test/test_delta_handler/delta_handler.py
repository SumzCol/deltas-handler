# -*- coding: utf-8 -*-

from sqlite3.dbapi2 import Cursor

def delta_handler(cursor:Cursor, origin_table:str, modified_table:str) -> str:
    """Function under development"""
    return "create table resultado (result int)"

def delta_retrieval(cursor:Cursor, origin_table:str, modified_table:str) -> str:
    """Function under development"""
    return "create table resultado (result int)"

#
#
#
#def create_database(cursor):
#    # drop tables
#    cursor.execute("""drop table if exists CLIENTS1""")
#    cursor.execute("""drop table if exists CLIENTS2""")
#    cursor.execute("""drop table if exists CLIENTS3""")
#    cursor.execute("""drop table if exists CLIENTS1_MOD""")
#    cursor.execute("""drop table if exists CLIENTS2_MOD""")
#    cursor.execute("""drop table if exists CLIENTS3_MOD""")
#    cursor.execute("""drop table if exists MODIFICATIONS_CLIENTS1""")
#    cursor.execute("""drop table if exists MODIFICATIONS_CLIENTS2""")
#    cursor.execute("""drop table if exists MODIFICATIONS_CLIENTS3""")
#
#    # create table clients1_mod
#    cursor.execute(
#        """
#        CREATE TABLE CLIENTS1 (
#            [generated_id] INTEGER PRIMARY KEY
#            , [client_name] text
#            , [value] integer
#            , [create_date] date
#            , [update_date] date
#        )"""
#    )
#
#    # create table clients1_mod
#    cursor.execute(
#        """
#        CREATE TABLE CLIENTS1_MOD (
#            [generated_id] INTEGER PRIMARY KEY
#            , [client_name] text
#            , [value] integer
#            , [create_date] date
#            , [update_date] date
#        )"""
#    )
#
#    # create table clients2
#    cursor.execute(
#        """
#        CREATE TABLE CLIENTS2 (
#            [generated_id] INTEGER PRIMARY KEY
#            , [client_name] text
#            , [value] integer
#            , [create_date] date
#        )"""
#    )
#
#    # create table clients2_mod
#    cursor.execute(
#        """
#        CREATE TABLE CLIENTS2_mod (
#            [generated_id] INTEGER PRIMARY KEY
#            , [client_name] text
#            , [value] integer
#            , [create_date] date
#        )"""
#    )
#
#    # create table clients3
#    cursor.execute(
#        """
#        CREATE TABLE CLIENTS3 (
#            [generated_id] INTEGER PRIMARY KEY
#            , [client_name] text
#            , [value] integer
#        )"""
#    )
#
#    # create table clients3_mod
#    cursor.execute(
#        """
#        CREATE TABLE CLIENTS3_MOD (
#            [generated_id] INTEGER PRIMARY KEY
#            , [client_name] text
#            , [value] integer
#        )"""
#    )
#    
#    # create modifications for each table
#    cursor.execute(
#        """
#        CREATE TABLE MODIFICATIONS_CLIENTS1 (
#            [generated_id] INTEGER PRIMARY KEY
#            , [client_name] text
#            , [value] integer
#            , [create_date] date
#            , [update_date] date
#            , [type_of_crud] text
#        )"""
#    )      
#
#    cursor.execute(
#        """
#        CREATE TABLE MODIFICATIONS_CLIENTS2 (
#            [generated_id] INTEGER PRIMARY KEY
#            , [client_name] text
#            , [value] integer
#            , [create_date] date
#            , [type_of_crud] text
#        )"""
#    )
#
#    cursor.execute(
#        """
#        CREATE TABLE MODIFICATIONS_CLIENTS3 (
#            [generated_id] INTEGER PRIMARY KEY
#            , [client_name] text
#            , [value] integer
#            , [type_of_crud] text
#        )"""
#    )
#    
#    yield cursor
#
#@pytest.fixture
#def create_databases():
#    """
#    Fixture to create databases
#    Args:
#        None
#    Returns:
#        None
#    """
#    create_database(firebase_connection)
#    create_database(mysql_connection)
#    create_database(postgresql_connection)
#
## Test 
#
## end of create_database()
#
## create faker to create fake data
#fake = Faker()
#
## test 1: simple insert
#def test_insert(create_databases):
#    """Simple test to check for inserts"""
#
#    cursor = create_database
#
#    # create data
#    client_test1 = [
#        (1, "Jose", 100, "2021-01-01", "2021-07-01"),
#        (2, "David", 200, "2021-01-01", "2021-07-01"),
#        (3, "Pablo", 300, "2021-01-01", "2021-07-01"),
#        (5, "Esteban", 500, "2021-01-01", "2021-07-01"),
#    ]
#
#    cursor.executemany(
#        "INSERT INTO clients1 VALUES(?, ?, ?, ?, ?)", 
#        client_test1
#    )
#    
#    # inserted data 
#    insert = [(4, "Camilo", 400, "2021-08-01", "2021-08-01")]
#    client_test1 += insert
#
#    cursor.executemany(
#        "INSERT INTO clients1_mod VALUES(?, ?, ?, ?, ?)", 
#        client_test1
#    )   
#
#    # create modifications
#    cursor.executemany(
#        "INSERT INTO MODIFICATIONS_CLIENTS1 VALUES(?, ?, ?, ?, ?, ?)", 
#        [insert[0] + ("insert",)]
#    )
#
#    # test
#    cursor.execute(delta_handler(cursor, "client1", "clients1_mod"))
#    assert len(list(cursor.execute("""SELECT * FROM MODIFICATIONS_CLIENTS1 EXCEPT SELECT * FROM resultado"""))) == 0
#
## end of test_insert()
#
#
## test 2: simple delete
#def test_delete(create_databases):
#    """Simple test to check for delete"""
#
#    cursor = create_database
#
#    # create data
#    client_test2 = [
#        (1, "Jose", 100, "2021-01-01", "2021-07-01"),
#        (2, "David", 200, "2021-01-01", "2021-07-01"),
#        (3, "Pablo", 300, "2021-01-01", "2021-07-01"),
#        (5, "Esteban", 500, "2021-01-01", "2021-07-01"),
#    ]
#
#    cursor.executemany(
#        "INSERT INTO clients1 VALUES(?, ?, ?, ?, ?)", 
#        client_test2
#    )
#
#    # deleted data
#    delete = [client_test2[2] + ("delete",)]
#    client_test2 = client_test2[:2]
#
#    cursor.executemany(
#        "INSERT INTO clients1_mod VALUES(?, ?, ?, ?, ?)", 
#        client_test2
#    )
#
#    # create modifications
#    cursor.executemany(
#        "INSERT INTO MODIFICATIONS_CLIENTS1 VALUES(?, ?, ?, ?, ?, ?)", 
#        delete
#    )
#
#    # test
#    cursor.execute(delta_handler(cursor, "client1", "clients1_mod"))
#    assert len(list(cursor.execute("""SELECT * FROM MODIFICATIONS_CLIENTS1 EXCEPT SELECT * FROM resultado"""))) == 0
#
## end of test_delete()
#
## test 3: simple update
#def test_update(create_databases):
#    """Simple test to check for updates"""
#
#    cursor = create_database
#
#    # create data
#    client_test3 = [
#        (1, "Jose", 100, "2021-01-01", "2021-07-01"),
#        (2, "David", 200, "2021-01-01", "2021-07-01"),
#        (3, "Pablo", 300, "2021-01-01", "2021-07-01"),
#        (5, "Esteban", 500, "2021-01-01", "2021-07-01"),
#    ]
#
#    cursor.executemany(
#        "INSERT INTO clients1 VALUES(?, ?, ?, ?, ?)", 
#        client_test3
#    )
#
#    # update data
#    update = (3, "Pablo", 900, "2021-01-01", "2021-08-01")
#    client_test3[2] = update
#
#    cursor.executemany(
#        "INSERT INTO clients1_mod VALUES(?, ?, ?, ?, ?)", 
#        client_test3
#    )
#
#    # create modifications
#    cursor.executemany(
#        "INSERT INTO MODIFICATIONS_CLIENTS1 VALUES(?, ?, ?, ?, ?, ?)", 
#        [update+("update",)]
#    )
#
#    # test
#    cursor.execute(delta_handler(cursor, "client1", "clients1_mod"))
#    assert len(list(cursor.execute("""SELECT * FROM MODIFICATIONS_CLIENTS1 EXCEPT SELECT * FROM resultado"""))) == 0
#
## end of test_update()
#
## test 4: no crud
#def test_no_modifications(create_databases):
#    """Simple test to check no crud operations"""
#
#    cursor = create_database
#
#    # create data
#    client_test4 = [
#        (1, "Jose", 100, "2021-01-01", "2021-07-01"),
#        (2, "David", 200, "2021-01-01", "2021-07-01"),
#        (3, "Pablo", 300, "2021-01-01", "2021-07-01"),
#        (5, "Esteban", 500, "2021-01-01", "2021-07-01"),
#    ]
#
#    cursor.executemany(
#        "INSERT INTO clients1 VALUES(?, ?, ?, ?, ?)", 
#        client_test4
#    )
#
#    # create unmodified data
#    cursor.executemany(
#        "INSERT INTO clients1_mod VALUES(?, ?, ?, ?, ?)", 
#        client_test4
#    )
#
#    # test
#    cursor.execute(delta_handler(cursor, "client1", "clients1_mod"))
#    assert len(list(cursor.execute("""SELECT * FROM resultado EXCEPT SELECT * FROM MODIFICATIONS_CLIENTS1"""))) == 0
#
## end of test_no_modifications()
#
## test 4.1: all new info
#def test_all_modifications(create_databases):
#    """Simple test to check full delete and insert"""
#
#    cursor = create_database
#    numbers = 1000
#
#    # create data
#    client_test41 = [(
#        fake.unique.random_int(min=1000000000, max=1500000000),
#        fake.unique.name(),
#        fake.unique.random_int(min=1000000, max=9999999),
#        fake.date_between(end_date=datetime.date(2020,12,31)),
#        ) for _ in range(numbers)
#    ]
#
#    cursor.executemany(
#        "INSERT INTO clients2 VALUES(?, ?, ?, ?)", 
#        client_test41
#    )
#
#    # save deleted data
#    modifications = [client + ("delete", ) for client in client_test41]
#
#    # create new data
#    client_test41 = [(
#            fake.unique.random_int(min=1000000000, max=1500000000),
#            fake.unique.name(),
#            fake.unique.random_int(min=1000000, max=9999999),
#            fake.date_between(end_date=datetime.date(2020,12,31)),
#            ) for _ in range(numbers)
#        ]
#
#    cursor.executemany(
#        "INSERT INTO clients2_mod VALUES(?, ?, ?, ?)", 
#        client_test41
#    )
#
#    # insert modifications
#    modifications = [client + ("insert", ) for client in client_test41] + \
#        modifications
#    
#    cursor.executemany(
#        "INSERT INTO MODIFICATIONS_CLIENTS2 VALUES(?, ?, ?, ?, ?)", 
#        modifications
#    )
#
#    # test
#    cursor.execute(delta_handler(cursor, "client2", "clients2_mod"))
#    assert len(list(cursor.execute("""SELECT * FROM MODIFICATIONS_CLIENTS2 EXCEPT SELECT * FROM resultado"""))) == 0
#
## end of test_all_modifications()
#
#
## test 5: updates without update date
#def test_update_no_dates(create_databases):
#    """Check for updates without update date"""
#
#    cursor = create_database
#    fake.unique.clear()
#    numbers = 100
#
#    # create data
#    client_test5 = [(
#        fake.unique.random_int(min=1000000000, max=1500000000),
#        fake.unique.name(),
#        fake.unique.random_int(min=1000000, max=9999999),
#        fake.date_between(end_date=datetime.date(2020,12,31)),
#        ) for _ in range(numbers)
#    ]
#
#    cursor.executemany(
#        "INSERT INTO clients2 VALUES(?, ?, ?, ?)", 
#        client_test5
#    )
#
#    # update data
#    qt_of_updates = fake.unique.random_int(min=1, max=numbers/4)
#
#    random_updates = [fake.unique.random_int(min=0, max=numbers-1) for _ in range(qt_of_updates)]
#
#    updates = []
#    for random_update in random_updates:
#
#        # save data to modify
#        updates.append(client_test5[random_update])
#
#        client_test5[random_update] = (
#            client_test5[random_update][0],
#            client_test5[random_update][1],
#            client_test5[random_update][2] + fake.unique.random_int(min=100000, max=999999),
#            client_test5[random_update][3]
#        )
#        
#
#    cursor.executemany(
#        "INSERT INTO clients2_mod VALUES(?, ?, ?, ?)", 
#        client_test5
#    )
#
#    # create modifications
#    updates = [update + ("update",) for update in updates]
#
#    cursor.executemany(
#        "INSERT INTO MODIFICATIONS_CLIENTS2 VALUES(?, ?, ?, ?, ?)", 
#        updates
#    )
#
#    # test
#    cursor.execute(delta_handler(cursor, "client2", "clients1_mod2"))
#    assert len(list(cursor.execute("""SELECT * FROM MODIFICATIONS_CLIENTS2 EXCEPT SELECT * FROM resultado"""))) == 0
#
## end of test_update_no_dates()
#
## test 6: inserts and deletes without dates
#def test_insert_delete_no_dates(create_databases):
#    """Check for insert and delete without date"""
#
#    cursor = create_database
#    fake.unique.clear()
#    numbers = 100
#
#    # create data
#    client_test6 = [(
#        fake.unique.random_int(min=1000000000, max=1500000000),
#        fake.unique.name(),
#        fake.unique.random_int(min=1000000, max=9999999),
#        ) for _ in range(numbers)
#    ]
#
#    cursor.executemany(
#        "INSERT INTO clients3 VALUES(?, ?, ?)", 
#        client_test6
#    )
#
#    # insert and deleted data
#    qt_of_inserts = fake.unique.random_int(min=1, max=round(numbers/4,0))
#    qt_of_deletes = fake.unique.random_int(min=1, max=round(numbers/4,0))
#
#    inserts = [(
#        fake.unique.random_int(min=1000000000, max=1500000000),
#        fake.unique.name(),
#        fake.unique.random_int(min=1000000, max=9999999),
#        ) for _ in range(qt_of_inserts)
#    ]
#
#    delete_index = [fake.unique.random_int(min=0, max=numbers-1) for _ in range(qt_of_deletes)]
#
#    deletes = [client_test6[delete] for delete in delete_index]
#    client_test6 = [client_test6[num] for num in range(numbers) if num not in delete_index] + inserts
#    client_test6.sort()
#
#    cursor.executemany(
#        "INSERT INTO clients3_mod VALUES(?, ?, ?)", 
#        client_test6
#    )
#
#    # create modifications
#    modifications = [delete + ("delete",) for delete in deletes] + \
#        [insert + ("insert",) for insert in inserts]
#
#    cursor.executemany(
#        "INSERT INTO MODIFICATIONS_CLIENTS3 VALUES(?, ?, ?, ?)", 
#        modifications
#    )
#
#    # test
#    cursor.execute(delta_handler(cursor, "client3", "clients3_mod"))
#    assert len(list(cursor.execute("""SELECT * FROM MODIFICATIONS_CLIENTS3 EXCEPT SELECT * FROM resultado"""))) == 0
#
## end of test_insert_delete_no_dates()
#
## test 7: any crud with dates
#def test_any_crud(create_databases):
#    """Check for any crudes with update dates"""
#
#    cursor = create_database
#    fake.unique.clear()
#    numbers = 1000
#
#    # create data
#    client_test7 = [(
#        fake.unique.random_int(min=1000000000, max=1500000000),
#        fake.unique.name(),
#        fake.unique.random_int(min=1000000, max=9999999),
#        fake.date_between(end_date=datetime.date(2020,12,31)),
#        fake.date_between(datetime.date(2021,1,1), datetime.date(2021,7,31))
#        ) for _ in range(numbers)
#    ]
#
#    cursor.executemany(
#        "INSERT INTO clients1 VALUES(?, ?, ?, ?, ?)", 
#        client_test7
#    )
#
#    # updated, inserted and deleted data
#    qt_of_updates = fake.unique.random_int(min=1, max=round(numbers/4,0))
#    qt_of_inserts = fake.unique.random_int(min=1, max=round(numbers/4,0))
#    qt_of_deletes = int(round((qt_of_updates + qt_of_inserts)/2,0))
#
#    random_updates = [fake.unique.random_int(min=0, max=numbers-1) for _ in range(qt_of_updates)]
#
#    updates = [] 
#    for random_update in random_updates:
#
#        updates.append(client_test7[random_update])
#
#        client_test7[random_update] = (
#            client_test7[random_update][0],
#            client_test7[random_update][1],
#            client_test7[random_update][2] + fake.unique.random_int(min=100000, max=999999),
#            client_test7[random_update][3],
#            fake.date_between(datetime.date(2021,8,1), datetime.date(2021,8,31))
#        )
#        
#
#    inserts = [(
#        fake.unique.random_int(min=1000000000, max=1500000000),
#        fake.unique.name(),
#        fake.unique.random_int(min=1000000, max=9999999),
#        fake.date_between(datetime.date(2021,1,1), datetime.date(2021,7,31)),
#        fake.date_between(datetime.date(2021,8,1), datetime.date(2021,8,31))
#        ) for _ in range(qt_of_inserts)
#    ]
#
#    fake.unique.clear()
#    delete_index = [fake.unique.random_int(min=0, max=numbers-1) for _ in range(qt_of_deletes)]
#    delete_index = [index for index in delete_index if index not in random_updates]
#
#    deletes = [client_test7[index] for index in delete_index]
#
#    client_test7 = [client_test7[num] for num in range(numbers) if num not in delete_index] + inserts
#
#    client_test7.sort()
#
#    cursor.executemany(
#        "INSERT INTO clients1_mod VALUES(?, ?, ?, ?, ?)", 
#        client_test7
#    )
#
#    # create modifications
#    modifications = [delete + ("delete",) for delete in deletes] + \
#        [insert + ("insert",) for insert in inserts] + \
#        [update + ("update",) for update in updates]
#
#    cursor.executemany(
#        "INSERT INTO MODIFICATIONS_CLIENTS1 VALUES(?, ?, ?, ?, ?, ?)", 
#        modifications
#    )
#
#    # test
#    cursor.execute(delta_handler(cursor, "client1", "clients1_mod"))
#    assert len(list(cursor.execute("""SELECT * FROM MODIFICATIONS_CLIENTS1 EXCEPT SELECT * FROM resultado"""))) == 0
#
## end of test_any_crud()
#
#
## test 8: any crud without dates
#def test_any_crud_no_dates(create_databases):
#    """Check for any crudes without dates"""
#
#    cursor = create_database
#    fake.unique.clear()
#    numbers = 1000
#
#    # create data
#    client_test8 = [(
#        fake.unique.random_int(min=1000000000, max=1500000000),
#        fake.unique.name(),
#        fake.unique.random_int(min=1000000, max=9999999),
#        ) for _ in range(numbers)
#    ]
#
#    cursor.executemany(
#        "INSERT INTO clients3 VALUES(?, ?, ?)", 
#        client_test8
#    )
#
#    # update data
#    qt_of_updates = fake.unique.random_int(min=1, max=round(numbers/4,0))
#    qt_of_inserts = fake.unique.random_int(min=1, max=round(numbers/4,0))
#    qt_of_deletes = int(round((qt_of_updates + qt_of_inserts)/2,0))
#
#    random_updates = [fake.unique.random_int(min=0, max=numbers-1) for _ in range(qt_of_updates)]
#
#    updates = []
#    for random_update in random_updates:
#
#        updates.append(client_test8[random_update])
#
#        client_test8[random_update] = (
#            client_test8[random_update][0],
#            client_test8[random_update][1],
#            client_test8[random_update][2] + fake.unique.random_int(min=100000, max=999999),
#        )
#        
#
#    inserts = [(
#        fake.unique.random_int(min=1000000000, max=1500000000),
#        fake.unique.name(),
#        fake.unique.random_int(min=1000000, max=9999999),
#        ) for _ in range(qt_of_inserts)
#    ]
#
#    fake.unique.clear()
#    delete_index = [fake.unique.random_int(min=0, max=numbers-1) for _ in range(qt_of_deletes)]
#    delete_index = [index for index in delete_index if index not in random_updates]
#
#    deletes = [client_test8[index] for index in delete_index]
#
#    client_test8 = [client_test8[num] for num in range(numbers) if num not in delete_index] + inserts
#
#    client_test8.sort()
#
#    cursor.executemany(
#        "INSERT INTO clients3_mod VALUES(?, ?, ?)", 
#        client_test8
#    )
#
#    # create modifications
#    modifications = [delete + ("delete",) for delete in deletes] + \
#        [insert + ("insert",) for insert in inserts] + \
#        [update + ("update",) for update in updates]
#
#    cursor.executemany(
#        "INSERT INTO MODIFICATIONS_CLIENTS3 VALUES(?, ?, ?, ?)", 
#        modifications
#    )
#
#    # test
#    cursor.execute(delta_handler(cursor, "client3", "clients3_mod"))
#    assert len(list(cursor.execute("""SELECT * FROM MODIFICATIONS_CLIENTS3 EXCEPT SELECT * FROM resultado"""))) == 0
#
## end of test_any_crud_no_dates()
