import mysql.connector
from constants import Constants


class Queries(object):
    """Database queries"""
    connection = mysql.connector.connect(user=Constants.USER, password=Constants.PASSWORD, database=Constants.DATABASE)
    cursor = connection.cursor()
    queries = []

    """Retrieve the names and genders of all people associated with ARC (i.e., members, employees, etc.)"""
    query1 = """
        SELECT 
            name, gender 
        FROM 
            person;
    """
    queries.append(query1)

    """List the names and departments of all “Faculty” members who are also members of ARC. """
    query2 = """
        SELECT
            name, department
        FROM
            non_student as n_s
        INNER JOIN
            university_affiliate as u_a ON n_s.card_id = u_a.card_id
        INNER JOIN
            person as p ON n_s.card_id = p.card_id
        WHERE
            member_type = "Faculty";
    """
    queries.append(query2)

    """Find the names of the people who have attended all events."""
    query4 = """
        SELECT
            name
        FROM
            attends as a
        INNER JOIN
            person as p ON a.card_id = p.card_id
        GROUP BY
            a.card_id
        HAVING
            COUNT(a.event_id) = (
                SELECT
                    COUNT(*)
                FROM
                    events
            );
    """
    queries.append(query4)

    """List the ID of events whose capacity have reached the maximum capacity of their associated space."""
    query5 = """
        SELECT
            event_id
        FROM
            events as e
        INNER JOIN
            space as s ON e.space_id = s.space_id
        WHERE capacity >= max_capacity;
    """
    queries.append(query5)

    """Find the names of students who have used all the equipment located in the cardio room. """
    query6 = """
        SELECT
            name
        FROM
            usage_reading as u_r
        INNER JOIN
            person as p ON u_r.card_id = p.card_id
        INNER JOIN
            equipment as e ON u_r.equipment_id = e.equipment_id
        INNER JOIN
            space as s ON e.space_id = s.space_id
        GROUP BY
            u_r.card_id
        HAVING
            COUNT(DISTINCT CASE WHEN desption = "cardio room" THEN u_r.equipment_id END) = (
                SELECT
                    COUNT(*)
                FROM
                    equipment as e2 
                INNER JOIN
                    space as s2 ON e2.space_id = s2.space_id
                WHERE
                    description = "cardio room"
            ); 
    """
    queries.append(query6)

    """List the equipment ids and types for equipment that is currently available."""
    query7 = """
        SELECT
            equipment_id, equipment_type
        FROM
            equipment as e
        WHERE
            is_available = TRUE;
    """
    queries.append(query7)

    """Find names of all employees in ARC."""
    query8 = """
        SELECT
            name
        FROM
            employee as e
        INNER JOIN 
            person as p ON e.card_id = p.card_id;
    """
    queries.append(query8)


    """Calculate the average hourly rate paid to all employees who are of student type at ARC"""
    query11 = """
        SELECT
            AVG(salary_hour)
        FROM
            employee
        WHERE
            employee_type = "student";
    """
    queries.append(query11)

    """Find the name of the Trainer(s) with the 2nd highest average hourly rate"""
    query12 = """
        SELECT
            name
        FROM
            Trainer as t
        INNER JOIN
            employee as e ON t.person_id = e.card_id
        INNER JOIN
            person as p ON e.card_id = p.card_id
        WHERE
            salary_hour = (
                SELECT
                    MAX(salary_hour)
                FROM
                    employee
                WHERE
                    salary_hour < (
                        SELECT
                            MAX(salary_hour)
                        FROM
                            employee
                    )
            );
    """
    queries.append(query12)

    """Find the ID of university affiliate(s) that have the highest number of family members that are ARC’s members."""
    query13 = """
        SELECT
            familyof
        FROM
            family
        GROUP BY 
            familyof
        HAVING
            COUNT(*) = (
                SELECT
                    MAX(x.num)
                FROM (
                    SELECT
                        COUNT(*) as num
                    FROM
                        family
                    GROUP BY
                        familyof
                ) x
            );
    """
    queries.append(query13)

    """Find the ID of university affiliate(s) that attends the most events"""
    query14 = """
        SELECT
            u_a.card_id
        FROM 
            university_affiliate as u_a
        INNER JOIN
            attends as a ON u_a.card_id = a.card_id
        GROUP BY 
            u_a.card_id
        HAVING
            COUNT(a.event_id) = (
                SELECT
                    MAX(x.uaev)
                FROM(
                    SELECT
                        COUNT(a.event_id) as uaev
                    FROM
                        university_affiliate as u_a
                    INNER JOIN
                        attends as a ON u_a.card_id = a.card_id
                    GROUP BY 
                        u_a.card_id
                ) x    
            );
    """
    queries.append(query14)

    """Find the spaces which have the lowest average occupancy per event."""
    query18 = """
        SELECT
            MIN(y.avgocc)
        FROM(
            SELECT
                AVG(z.occt) as avgocc
            FROM (
                SELECT
                    count(card_id) as occt
                FROM
                    attends as a

                INNER JOIN events as e ON a.event_id = e.event_id

                GROUP BY 
                    a.event_id
                )z
            GROUP BY 
                space_id
            )y
    """
    queries.append(query18)

    for i, query in enumerate(queries):
        cursor.execute(query)
        print(f"{i + 1}. {cursor.fetchall()}")
