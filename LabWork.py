import sqlite3 as sl


def get_connection():
    conn = sl.connect('company_database')
    conn.execute('Pragma foreign_keys=ON')
    return conn


def reset_database():
    with get_connection() as conn:
        cursor = conn.cursor()
        # Удаление записей из EmployeeCard, которые ссылаются на записи в Person
        cursor.execute("DELETE FROM EmployeeCard WHERE person_id IN (SELECT person_id FROM Person)")
        # Удаление записей из Person
        cursor.execute("DELETE FROM Person")
        # Удаление записей из Projects
        cursor.execute("DELETE FROM Projects")
        # Удаление записей из Groups
        cursor.execute("DELETE FROM Groups")
        # Удаление записей из EmployeeCard, которые ссылаются на записи в Projects и Groups
        cursor.execute("DELETE FROM EmployeeCard WHERE project_id IN (SELECT project_id FROM Projects)")
        cursor.execute("DELETE FROM EmployeeCard WHERE group_id IN (SELECT group_id FROM Groups)")
        # Удаление таблиц
        cursor.execute("DROP TABLE IF EXISTS Person")
        cursor.execute("DROP TABLE IF EXISTS Projects")
        cursor.execute("DROP TABLE IF EXISTS Groups")
        cursor.execute("DROP TABLE IF EXISTS EmployeeCard")
        print("Database reset successfully.")



def print_table_contents(table_name):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        print(f"\nContents of {table_name}:")
        for row in rows:
            print(row)


def create_tables() -> None:
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = '''
            CREATE TABLE IF NOT EXISTS Person ( 
                person_id INT PRIMARY KEY,
                first_name TEXT NOT NULL,
                second_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            );
            CREATE TABLE IF NOT EXISTS Projects ( 
                project_id INT PRIMARY KEY,
                project_name TEXT NOT NULL,
                start_date DATE,
                end_date DATE  
            );
            CREATE TABLE IF NOT EXISTS Groups ( 
                  group_id INT PRIMARY KEY,
                  group_name TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS EmployeeCard ( 
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
        cursor.executescript(sql)


def add_person(person_data):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = '''INSERT OR REPLACE INTO Person (person_id, first_name, second_name, email)
                 VALUES (?,?,?,?)'''
        cursor.execute(sql, person_data)


def add_project(project_data):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """INSERT OR REPLACE INTO Projects (project_id, project_name, start_date, end_date)
                 VALUES (?, ?, ?, ?)"""
        cursor.execute(sql, project_data)


def add_group(group_data):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """INSERT OR REPLACE INTO Groups (group_id, group_name)
                 VALUES (?, ?)"""
        cursor.execute(sql, group_data)


def add_employee_card(card_data):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """INSERT OR REPLACE INTO EmployeeCard (card_id, person_id, project_id, group_id, start_date, end_date)
                 VALUES (?, ?, ?, ?, ?, ?)"""
        cursor.execute(sql, card_data)


def delete_person(person_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = "DELETE FROM Person WHERE person_id = ?"
        cursor.execute(sql, (person_id,))


def delete_project(project_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = "DELETE FROM Projects WHERE project_id = ?"
        cursor.execute(sql, (project_id,))


def delete_group(group_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = "DELETE FROM Groups WHERE group_id = ?"
        cursor.execute(sql, (group_id,))


def delete_employee_card(card_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = "DELETE FROM EmployeeCard WHERE card_id = ?"
        cursor.execute(sql, (card_id,))


def update_person(person_data):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """UPDATE Person SET 
                 first_name = ?, 
                 second_name = ?, 
                 email = ? 
                 WHERE person_id = ?"""
        cursor.execute(sql, person_data)


def update_project(project_data):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """UPDATE Projects SET 
                 project_name = ?, 
                 start_date = ?, 
                 end_date = ? 
                 WHERE project_id = ?"""
        cursor.execute(sql, project_data)


def update_group(group_data):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """UPDATE Groups SET 
                 group_name = ? 
                 WHERE group_id = ?"""
        cursor.execute(sql, group_data)


def update_employee_card(card_data):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """UPDATE EmployeeCard SET 
                 person_id = ?, 
                 project_id = ?, 
                 group_id = ?, 
                 start_date = ?, 
                 end_date = ? 
                 WHERE card_id = ?"""
        cursor.execute(sql, card_data)


def get_person_count():
    with get_connection() as conn:
        cursor = conn
        sql = """SELECT COUNT(*) FROM Person """
        cursor.execute(sql)
        print()


def count_persons():
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = "SELECT COUNT(*) FROM Person"
        cursor.execute(sql)
        result = cursor.fetchone()
        print(f"Total persons: {result[0]}")


def sum_project_durations():
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = "SELECT SUM(julianday(end_date) - julianday(start_date)) FROM Projects"
        cursor.execute(sql)
        result = cursor.fetchone()
        print(f"Total project duration in days: {result[0]}")


def average_project_duration():
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = "SELECT AVG(julianday(end_date) - julianday(start_date)) FROM Projects"
        cursor.execute(sql)
        result = cursor.fetchone()
        print(f"Average project duration in days: {result[0]}")


def max_project_end_date():
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = "SELECT MAX(end_date) FROM Projects"
        cursor.execute(sql)
        result = cursor.fetchone()
        print(f"Latest project end date: {result}")


def min_project_start_date():
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = "SELECT MIN(start_date) FROM Projects"
        cursor.execute(sql)
        result = cursor.fetchone()
        print(f"Earliest project start date: {result[0]}")


def convert_date_to_string(date):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = "SELECT date(?) AS date_string"
        cursor.execute(sql, (date,))
        result = cursor.fetchone()
        return result[0]


def group_projects_by_duration():
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """
            CREATE TABLE IF NOT EXISTS ProjectDurationSummary AS
            SELECT 
                project_id, 
                project_name, 
                julianday(end_date) - julianday(start_date) AS duration_days
            FROM Projects
            GROUP BY project_id, project_name
            HAVING duration_days > 150
            ORDER BY duration_days DESC;
        """
        cursor.executescript(sql)
        print("ProjectDurationSummary table created.")


def sort_projects_by_start_date():
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """
            CREATE TABLE IF NOT EXISTS SortedProjects AS
            SELECT * FROM Projects
            ORDER BY start_date ASC;
        """
        cursor.executescript(sql)
        print("SortedProjects table created.")


def create_table_with_names_containing_a():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS NamesWithA AS
            SELECT first_name, second_name
            FROM Person
            WHERE first_name LIKE 'A%' OR second_name LIKE 'A%';
        """)
        print("Table NamesWithA created with names containing 'А'.")


if __name__ == '__main__':
    create_tables()
    persons = [
        [1, 'John', 'Doe', 'john.doe@example.com'],
        [2, 'Anne', 'Doe', 'jane.doe@example.com'],
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
        add_person(person)
    for project in projects:
        add_project(project)
    for group in groups:
        add_group(group)
    for card in employee_cards:
        add_employee_card(card)

    # reset_database()
    create_tables()

    # # Тестирование функций агрегации
    # print("Testing aggregation functions:")
    # count_persons()
    # sum_project_durations()
    # average_project_duration()
    # max_project_end_date()
    # min_project_start_date()
    #
    # # Тестирование функции преобразования типов данных
    # print("\nTesting date conversion function:")
    # date_string = convert_date_to_string('2023-01-01')
    # print(f"Converted date string: {date_string}")
    #
    # # Тестирование функций группировки данных
    # print("\nTesting data grouping function:")
    # group_projects_by_duration()
    #
    # # Тестирование функции сортировки результатов запроса
    # print("\nTesting query sorting function:")
    # sort_projects_by_start_date()
    #
    # print("\n sort_projects_by_start_date")
    # create_table_with_names_containing_a()

    update_person([2, 'Anne', 'UpdatedDoe', 'jane.updated@example.com'])
    update_project([2, 'UpdatedProject B', '2023-02-01', '2023-07-31'])
    update_group([2, 'UpdatedGroup 2'])
    update_employee_card([2, 2, 2, 2, '2023-02-01 00:00:00', '2023-07-31 23:59:59'])

    print_table_contents("Person")
    print_table_contents("Projects")
    print_table_contents("Groups")
    print_table_contents("EmployeeCard")
