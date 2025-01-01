-- ## Table: employees
-- CREATE TABLE employees (
--     emp_id INT PRIMARY KEY,
--     name VARCHAR(50),
--     department_id INT
-- );

-- INSERT INTO employees VALUES
-- (1, 'Karthik', 101),
-- (2, 'Veena', 102),
-- (3, 'Meena', NULL),
-- (4, 'Veer', 103),
-- (5, 'Ajay', 104),
-- (6, 'Vijay', NULL),
-- (7, 'Keerthi', 105);

-- ## Table: departments
-- CREATE TABLE departments (
--     department_id INT PRIMARY KEY,
--     department_name VARCHAR(50)
-- );

-- INSERT INTO departments VALUES
-- (101, 'HR'),
-- (102, 'Finance'),
-- (103, 'IT'),
-- (104, 'Marketing'),
-- (106, 'Operations');

-- 1.	List all employees and their department names, but only include employees assigned to a department.

SELECT emp_id, name, department_name
FROM employees as e
INNER JOIN departments as d
ON e.department_id = d.department_id;

-- 2.	Find employees whose department name starts with the letter 'F'.

SELECT emp_id, name, department_name
FROM employees as e
INNER JOIN departments as d
ON e.department_id = d.department_id
WHERE department_name LIKE 'F%';

-- 6.	List all employees, including those without a department, and their department details.

SELECT emp_id, name, department_name;

-- 7.	Find employees not assigned to any department.

-- ------------------------------------------------------
-- INNER JOIN Questions
-- 1. List all employees and their department names, but only include employees assigned to a
-- department.

SELECT e.emp_id, name, d.department_name
FROM employees AS e
JOIN departments AS d
on e.department_id = d.department_id
WHERE e.department_id IS NOT NULL;

-- 2. Find employees whose department name starts with the letter 'F'.

SELECT e.name, d.department_name
FROM employees AS e
JOIN departments AS d
ON e.department_id = d.department_id
WHERE d.department_name LIKE "F%";

-- 3. Retrieve employee details along with the department name for departments with IDs
-- greater than 102.

SELECT e.emp_id, name, d.department_name, e.department_id
FROM employees AS e
JOIN departments AS d
ON e.department_id = d.department_id
WHERE d.department_id > 102;

-- 4. Find employees working in the 'IT' or 'Marketing' departments.

SELECT e.emp_id, name, d.department_name, e.department_id
FROM employees AS e
JOIN departments AS d
ON e.department_id = d.department_id
WHERE d.department_name IN ("IT","Marketing");

-- 5. List department names and the total number of employees in each department. (Hint: Use
-- GROUP BY with INNER JOIN)

SELECT d.department_name, COUNT(e.emp_id) AS total_num_of_employees
FROM employees AS e
JOIN departments AS d
ON e.department_id = d.department_id
GROUP BY d.department_name;

-- LEFT OUTER JOIN Questions
-- 6. List all employees, including those without a department, and their department details.

SELECT e.emp_id, e.name, e.department_id , d.department_name
FROM employees e
LEFT JOIN departments d
ON e.department_id = d.department_id;

-- 7. Find employees not assigned to any department.

SELECT e.emp_id, e.name, e.department_id , d.department_name
FROM employees e
LEFT JOIN departments d
ON e.department_id = d.department_id
WHERE e.department_id IS NULL;

-- 8. Fetch all employees and their departments, replacing missing department names with 'Not
-- Assigned'.

SELECT e.emp_id, e.name, e.department_id , COALESCE(d.department_name,"Not Assigned") AS updated_department_name
FROM employees e
LEFT JOIN departments d
ON e.department_id = d.department_id;

SELECT e.emp_id, e.name, e.department_id , IFNULL(d.department_name,"Not Assigned") AS updated_department_name
FROM employees e
LEFT JOIN departments d
ON e.department_id = d.department_id;

-- 9. List employees along with department details for those working in departments with IDs
-- less than 104.

SELECT e.emp_id, e.name, e.department_id , d.department_name
FROM employees e
LEFT JOIN departments d
ON e.department_id = d.department_id;

-- 10. Find employees with names starting with 'V' and their corresponding departments (if
-- available).

SELECT e.emp_id, e.name, e.department_id , d.department_name
FROM employees e
LEFT JOIN departments d
ON e.department_id = d.department_id
WHERE e.name LIKE "V%";

-- RIGHT OUTER JOIN Questions
-- 11. List all departments and their employee details, including departments with no
-- employees.

SELECT e.emp_id, e.name, e.department_id , d.department_name
FROM employees e
RIGHT JOIN departments d
ON e.department_id = d.department_id;

-- 12. Find departments without any employees assigned to them.

SELECT e.emp_id, e.name, e.department_id , d.department_name
FROM employees e
RIGHT JOIN departments d
ON e.department_id = d.department_id
WHERE e.emp_id IS NULL;

-- 13. Fetch the names of all departments along with employee names, where department
-- names contain the letter 'O'.

SELECT d.department_name, e.name AS employee_name
FROM employees e
RIGHT JOIN departments d
ON e.department_id = d.department_id
WHERE d.department_name LIKE '%O%';

-- 14. Retrieve department details along with employees whose names end with 'a'.

SELECT d.department_id, d.department_name, e.emp_id, e.name AS employee_name
FROM employees e
RIGHT JOIN departments d
ON e.department_id = d.department_id
WHERE e.name LIKE '%a';

-- 15. List all departments with fewer than two employees.

SELECT d.department_name, COUNT(e.emp_id) AS employee_count_per_department
FROM employees e
RIGHT JOIN departments d
ON e.department_id = d.department_id
GROUP BY d.department_name
HAVING COUNT(e.emp_id) < 2;

-- FULL OUTER JOIN Questions
-- 16. List all employees and departments, ensuring no record is missed.

SELECT e.name, d.department_name
FROM employees AS e
LEFT JOIN departments AS d
ON e.department_id = d.department_id

UNION

SELECT e.name, d.department_name
FROM employees AS e
RIGHT JOIN departments AS d
ON e.department_id = d.department_id;

-- 17. Find all departments and employees where there is no match between the two tables.
-- meena, vijay, keerthi, operations
SELECT e.name, d.department_name
FROM employees AS e
LEFT JOIN departments AS d
ON e.department_id = d.department_id
WHERE e.department_id IS NULL

UNION

SELECT e.name, d.department_name
FROM employees AS e
RIGHT JOIN departments AS d
ON e.department_id = d.department_id
WHERE e.emp_id IS NULL;

-- 18. Fetch details of all employees and departments, showing 'No Department' for missing
-- department details and 'No Employee' for missing employee details.

SELECT COALESCE(e.name, "No Employee") AS employees_details, IFNULL(d.department_name,"No Department") AS departments_details
FROM employees AS e
LEFT JOIN departments AS d
ON e.department_id = d.department_id

UNION

SELECT COALESCE(e.name, "No Employee") AS employees_details, IFNULL(d.department_name,"No Department") AS departments_details
FROM employees AS e
RIGHT JOIN departments AS d
ON e.department_id = d.department_id;

-- 19. List employees and departments, ensuring departments with names ending in 's' are
-- included even if no employees are assigned.

SELECT e.name, d.department_name
FROM employees AS e
LEFT JOIN departments AS d
ON e.department_id = d.department_id

UNION

SELECT e.name, d.department_name
FROM employees AS e
RIGHT JOIN departments AS d
ON e.department_id = d.department_id
WHERE d.department_name LIKE '%s';

-- 20. Find departments and employees where department_id does not match, showing
-- mismatched rows explicitly.

SELECT e.name, d.department_name
FROM employees AS e
LEFT JOIN departments AS d
ON e.department_id = d.department_id
WHERE e.department_id IS NULL

UNION

SELECT e.name, d.department_name
FROM employees AS e
RIGHT JOIN departments AS d
ON e.department_id = d.department_id
WHERE d.department_id IS NULL;

-- ------------------------------------------------------



