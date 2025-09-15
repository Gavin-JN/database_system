-- 数据库系统测试SQL语句

-- 创建学生表
CREATE TABLE student(id INT, name VARCHAR, age INT, grade VARCHAR);

-- 创建课程表
CREATE TABLE course(id INT, title VARCHAR, credits INT);

-- 插入学生数据
INSERT INTO student(id,name,age,grade) VALUES (1,'Alice',20,'A');
INSERT INTO student(id,name,age,grade) VALUES (2,'Bob',22,'B');
INSERT INTO student(id,name,age,grade) VALUES (3,'Charlie',19,'A');
INSERT INTO student(id,name,age,grade) VALUES (4,'David',21,'C');
INSERT INTO student(id,name,age,grade) VALUES (5,'Eve',20,'B');

-- 插入课程数据
INSERT INTO course(id,title,credits) VALUES (1,'Database Systems',3);
INSERT INTO course(id,title,credits) VALUES (2,'Operating Systems',4);
INSERT INTO course(id,title,credits) VALUES (3,'Data Structures',3);

-- 查询所有学生
SELECT * FROM student;

-- 查询年龄大于20的学生
SELECT id,name,age FROM student WHERE age > 20;

-- 查询成绩为A的学生
SELECT name,grade FROM student WHERE grade = 'A';

-- 查询所有课程
SELECT * FROM course;

-- 查询学分大于3的课程
SELECT title,credits FROM course WHERE credits > 3;

-- 删除年龄小于20的学生
DELETE FROM student WHERE age < 20;

-- 再次查询所有学生
SELECT * FROM student;

-- 删除所有课程
DELETE FROM course;

-- 查询课程表（应该为空）
SELECT * FROM course;
