CREATE SCHEMA IF NOT EXISTS CAFETERIA;

CREATE TABLE CAFETERIA.canteen_orders (
    order_id TEXT,
    item_name TEXT,
    price TEXT,
    category TEXT,
    ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE CAFETERIA.cafe_sales (
    transaction_key TEXT,
    dish_name TEXT,
    amount_paid DECIMAL(8,2),
    meal_type TEXT,
    ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW CAFETERIA.cafeteria_sales_orders AS
SELECT
    'Canteen' as source_location,
    order_id::INTEGER AS order_id,
    item_name AS item_name,
    category,
    price::DECIMAL(8,2) AS price,
    ordered_at AS source_ordered_at
FROM CAFETERIA.canteen_orders
WHERE order_id IS NOT NULL

UNION ALL

SELECT
    'Cafe' AS source_location,
    transaction_key::INTEGER,
    dish_name,
    meal_type,
    amount_paid,
    ordered_at
FROM CAFETERIA.cafe_sales
WHERE transaction_key IS NOT NULL;

SELECT * FROM CAFETERIA.cafeteria_sales_orders;

CREATE TABLE FINANCE.student_fees_raw (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO FINANCE.student_fees_raw (raw_data) VALUES
('{"id": 2001, 
  "student_id": "STU2001",
  "level": 4,
  "fees": {
    "paid_till": 3,
    "pending": 4450.00,
    "scholarship_deductions": 2000.00,
    "scholarship_applicable": true
  }}'),

('{"id": 2002, 
  "student_id": "STU2002",
  "level": 5,
  "fees": {
    "paid_till": 5,
    "pending": 0.00,
    "scholarship_deductions": 1500.00,
    "scholarship_applicable": false
  }}'),

('{"id": 2003, 
  "student_id": "STU2003",
  "level": 6,
  "fees": {
    "paid_till": 2,
    "pending": 8100.00,
    "scholarship_deductions": 3000.00,
    "scholarship_applicable": true
  }}');

SELECT * FROM FINANCE.student_fees_raw;

 SELECT 
    (raw_data->>'id')::INTEGER as id,
    raw_data->>'student_id' as student_id,
    (raw_data->>'level')::INTEGER as level,
    (raw_data->'fees'->>'paid_till')::INTEGER as paid_till_semester,
    (raw_data->'fees'->>'pending')::DECIMAL(10,2) as pending,
    (raw_data->'fees'->>'scholarship_deductions')::DECIMAL(10,2) as scholarship_deductions,
(raw_data->'fees'->>'scholarship_applicable')::BOOLEAN as scholarship_applicable
FROM FINANCE.student_fees_raw 
WHERE raw_data IS NOT NULL;

CREATE SCHEMA IF NOT EXISTS ACADEMICS;

CREATE TABLE ACADEMICS.tutors_record (
    id SERIAL PRIMARY KEY,
    full_name TEXT,
    phone_raw TEXT,
    module TEXT
);

CREATE SCHEMA IF NOT EXISTS ACADEMICS;

CREATE TABLE ACADEMICS.tutors_record (
    id SERIAL PRIMARY KEY,
    full_name TEXT,
    phone_raw TEXT,
    module TEXT
);

INSERT INTO ACADEMICS.tutors_record (full_name, phone_raw, module) VALUES
('Deepson Shrestha', '98-11111111', 'Distributed Systems'),
('Dipesh Shrestha', '9822222.222', 'HCI'),
('Yogesh Bikram Shah', '(+977) 9833333333', 'Collaborative Development'),
('Sarayu', '9844444444', 'Distributed Systems');

SELECT 
    id,
    module,
    SPLIT_PART(full_name, ' ', 1) as tutor,
    CASE 
        WHEN length(regexp_replace(phone_raw, '[^0-9]', '', 'g')) >= 10
        THEN regexp_replace(phone_raw, '[^0-9]', '', 'g')
        ELSE NULL
    END as phone
FROM ACADEMICS.tutors_record;

CREATE TABLE ACADEMICS.student_marks (
    student_id INT,
    student_name VARCHAR(50),
    course_id VARCHAR(10),
    marks_obtained INT,
    PRIMARY KEY (student_id, course_id)
);

INSERT INTO ACADEMICS.student_marks (student_id, student_name, course_id, marks_obtained) VALUES
(101, 'AROJ MAHARJAN', '5CS019', 85),
(102, 'HARDIK SHRESTHA', '5CS019', 92),
(103, 'KAMAL SINGH', '5CS019', 85),
(104, 'DHITAL PRASAI', '5CS019', 78),
(105, 'LUJA DOBAL', '5CS019', 70),
(106, 'RATNA KC', '5CS019', 70),
(107, 'MANAN RATNA', '5CS021', 88),
(108, 'DHARNA SINGH', '5CS021', 75),
(109, 'NISCHAL DOBAL', '5CS021', 75),
(110, 'PRARAMBHA MAHARJAN', '5CS021', 65),
(111, 'RANJAN PRASAI', '5CS021', 55),
(112, 'RHYS SHRESTHA', '5CS021', 55),
(113, 'RIJEN KC', '5CS021', 38),
(101, 'AROJ MAHARJAN', '5CS020', 95),
(102, 'HARDIK SHRESTHA', '5CS020', 82),
(114, 'KAMAL MAHARJAN', '5CS020', 82),
(115, 'DHITAL SHRESTHA', '5CS020', 60),
(116, 'NISCHAL PRASAI', '5CS020', 60),
(117, 'HARDIK MANAN', '5CS020', 29),
(103, 'KAMAL SINGH', '5CS022', 91),
(104, 'DHITAL PRASAI', '5CS022', 80),
(118, 'AROJ SINGH', '5CS022', 80),
(119, 'LUJA SINGH', '5CS022', 80),
(120, 'RATNA DOBAL', '5CS022', 72),
(105, 'LUJA DOBAL', '5CS024', 94),
(106, 'RATNA KC', '5CS024', 94),
(107, 'MANAN RATNA', '5CS024', 85);

SELECT * FROM ACADEMICS.student_marks;


SELECT 
    student_id,
    student_name,
    course_id,
    marks_obtained,
    ROW_NUMBER() OVER (ORDER BY student_id) as record_number,
    ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY marks_obtained DESC) as position_in_course,
    RANK() OVER (PARTITION BY course_id ORDER BY marks_obtained DESC) as rank_with_ties_skipped,
    DENSE_RANK() OVER (PARTITION BY course_id ORDER BY marks_obtained DESC) as rank_without_gaps
FROM ACADEMICS.student_marks
ORDER BY course_id, marks_obtained DESC;

CREATE SCHEMA IF NOT EXISTS resource;
CREATE TABLE resource.resource_tracking (
    id SERIAL PRIMARY KEY,
    resource_name VARCHAR(100) NOT NULL,
    person_name VARCHAR(100) NOT NULL,
    taken_date DATE DEFAULT CURRENT_DATE,
    number_of_items INTEGER DEFAULT 1,
    departments TEXT[] DEFAULT ARRAY[]::TEXT[],
 total_departments INTEGER GENERATED ALWAYS AS (cardinality(departments)) STORED
);

INSERT INTO resource.resource_tracking (resource_name, person_name, taken_date, number_of_items, departments) VALUES
('Marker', 'Aarav Sharma', '2024-06-15', 5, ARRAY['IT-Academics', 'PAT']),
('Multiplug', 'Prerana Adhikari', '2024-06-20', 2, ARRAY['Business Development', 'Students Service']),
('Sticky Notes', 'Bibek Thapa', '2024-06-22', 10, ARRAY['RTE']),
('Diary', 'Anusha Koirala', '2024-06-25', 3, ARRAY[]::TEXT[]),
('Tissue', 'Sujal Bhandari', '2024-06-26', 1, ARRAY['Resource', 'RTE']);

SELECT 
   resource_name,
   person_name,
   total_departments,
  'IT-Academics' = ANY(departments) as is_it_academics,
  COALESCE(NULLIF(array_to_string(departments, ' | '), ''), 'Unassigned') as formatted_departments
FROM resource.resource_tracking;

