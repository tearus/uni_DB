import sqlite3 as sl


# noinspection SqlResolve
def create_tables(cursor) -> None:
    sql_queries = [
        '''CREATE TABLE IF NOT EXISTS Person ( 
            person_id INT PRIMARY KEY,
            first_name TEXT NOT NULL,
            second_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        );''',
        '''CREATE TABLE IF NOT EXISTS Projects ( 
            project_id INT PRIMARY KEY,
            project_name TEXT NOT NULL,
            start_date DATE,
            end_date DATE  
        );''',
        '''CREATE TABLE IF NOT EXISTS Groups ( 
              group_id INT PRIMARY KEY,
              group_name TEXT NOT NULL
        );''',
        '''CREATE TABLE IF NOT EXISTS EmployeeCard ( 
              card_id INT PRIMARY KEY,
              person_id INT,
              project_id INT,
              group_id INT,
              start_date TIMESTAMP DEFAULT 'now',
              end_date TIMESTAMP,
              FOREIGN KEY (person_id) REFERENCES Person(person_id),
              FOREIGN KEY (project_id) REFERENCES Projects(project_id),
              FOREIGN KEY (group_id) REFERENCES Groups(group_id)
        );'''
    ]
    for query in sql_queries:
        cursor.execute(query)


def add_person(cursor, person_data):
    sql = """INSERT INTO Person (person_id, first_name, second_name, email)
             VALUES (?, ?, ?, ?)"""
    cursor.execute(sql, person_data)


def add_project(cursor, project_data):
    sql = """INSERT INTO Projects (project_id, project_name, start_date, end_date)
             VALUES (?, ?, ?, ?)"""
    cursor.execute(sql, project_data)


def add_group(cursor, group_data):
    sql = """INSERT INTO Groups (group_id, group_name)
             VALUES (?, ?)"""
    cursor.execute(sql, group_data)


def add_employee_card(cursor, card_data):
    sql = """INSERT INTO EmployeeCard (card_id, person_id, project_id, group_id, start_date, end_date)
             VALUES (?, ?, ?, ?, ?, ?)"""
    cursor.execute(sql, card_data)


def delete_person(cursor, person_id):
    sql = "DELETE FROM Person WHERE person_id = ?"
    cursor.execute(sql, (person_id,))


def delete_project(cursor, project_id):
    sql = "DELETE FROM Projects WHERE project_id = ?"
    cursor.execute(sql, (project_id,))


def delete_group(cursor, group_id):
    sql = "DELETE FROM Groups WHERE group_id = ?"
    cursor.execute(sql, (group_id,))


def delete_employee_card(cursor, card_id):
    sql = "DELETE FROM EmployeeCard WHERE card_id = ?"
    cursor.execute(sql, (card_id,))


def update_person(cursor, person_data):
    sql = """UPDATE Person SET 
             first_name = ?, 
             second_name = ?, 
             email = ? 
             WHERE person_id = ?"""
    cursor.execute(sql, person_data)


def update_project(cursor, project_data):
    sql = """UPDATE Projects SET 
             project_name = ?, 
             start_date = ?, 
             end_date = ? 
             WHERE project_id = ?"""
    cursor.execute(sql, project_data)


def update_group(cursor, group_data):
    sql = """UPDATE Groups SET 
             group_name = ? 
             WHERE group_id = ?"""
    cursor.execute(sql, group_data)


def update_employee_card(cursor, card_data):
    sql = """UPDATE EmployeeCard SET 
             person_id = ?, 
             project_id = ?, 
             group_id = ?, 
             start_date = ?, 
             end_date = ? 
             WHERE card_id = ?"""
    cursor.execute(sql, card_data)


if __name__ == '__main__':
    con = sl.connect('company_database')
    c = con.cursor()
    create_tables(c)
    persons = [
        [1, 'John', 'Doe', 'john.doe@example.com'],
        [2, 'Jane', 'Doe', 'jane.doe@example.com'],
        [3, 'Alice', 'Smith', 'alice.smith@example.com'],
        [4, 'Bob', 'Johnson', 'bob.johnson@example.com'],
        [5, 'Charlie', 'Brown', 'charlie.brown@example.com'],
        [6, 'Diana', 'Prince', 'diana.prince@example.com'],
        [7, 'Eva', 'Miller', 'eva.miller@example.com']
    ]

    projects = [
        [1, 'Project A', '2023-01-01', '2023-12-31'],
        [2, 'Project B', '2023-02-01', '2023-06-30'],
        [3, 'Project C', '2023-07-01', '2023-12-31'],
        [4, 'Project D', '2023-01-01', '2023-06-30'],
        [5, 'Project E', '2023-07-01', '2023-12-31'],
        [6, 'Project F', '2023-01-01', '2023-06-30'],
        [7, 'Project G', '2023-07-01', '2023-12-31']
    ]

    groups = [
        [1, 'Group 1'],
        [2, 'Group 2'],
        [3, 'Group 3'],
        [4, 'Group 4'],
        [5, 'Group 5'],
        [6, 'Group 6'],
        [7, 'Group 7']
    ]

    employee_cards = [
        [1, 1, 1, 1, '2023-01-01 00:00:00', '2023-12-31 23:59:59'],
        [2, 2, 2, 2, '2023-02-01 00:00:00', '2023-06-30 23:59:59'],
        [3, 3, 3, 3, '2023-07-01 00:00:00', '2023-12-31 23:59:59'],
        [4, 4, 4, 4, '2023-01-01 00:00:00', '2023-06-30 23:59:59'],
        [5, 5, 5, 5, '2023-07-01 00:00:00', '2023-12-31 23:59:59'],
        [6, 6, 6, 6, '2023-01-01 00:00:00', '2023-06-30 23:59:59'],
        [7, 7, 7, 7, '2023-07-01 00:00:00', '2023-12-31 23:59:59']
    ]

    for person in persons:
        add_person(c, person)
    for project in projects:
        add_project(c, project)
    for group in groups:
        add_group(c, group)
    for card in employee_cards:
        add_employee_card(c, card)

    update_person(c, [2, 'Jane', 'UpdatedDoe', 'jane.updated@example.com'])
    update_project(c, [2, 'UpdatedProject B', '2023-02-01', '2023-07-31'])
    update_group(c, [2, 'UpdatedGroup 2'])
    update_employee_card(c, [2, 2, 2, 2, '2023-02-01 00:00:00', '2023-07-31 23:59:59'])

    con.commit()
    con.close()

