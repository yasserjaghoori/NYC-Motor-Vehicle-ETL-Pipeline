#!/usr/bin/env python
# coding: utf-8

# In[2]:


import mysql.connector

# Connect to your RDS instance
conn = mysql.connector.connect(
    host="host",
    user="username",
    password="password_here",  
    port='port here'
)

cursor = conn.cursor()

# Step 1: Create a new database
cursor.execute("CREATE DATABASE IF NOT EXISTS nyc_collision_project;")
print("Database 'nyc_collision_project' created (or already exists).")

# Step 2: Connect to the new database
conn.database = 'nyc_collision_project'

# Step 3: Create a new table
cursor.execute("""
CREATE TABLE IF NOT EXISTS collisions (
    collision_id INT PRIMARY KEY,
    crash_date DATE,
    crash_time TIME,
    borough VARCHAR(255),
    zip_code VARCHAR(10),
    latitude FLOAT,
    longitude FLOAT
);
""")
print("Table 'collisions' created (or already exists).")

# Clean up
conn.commit()
cursor.close()
conn.close()

print("Done!")

