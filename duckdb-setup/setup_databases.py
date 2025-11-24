"""
Setup script to create DuckDB databases for ACT and VRM systems.
This script creates the source databases with sample data including deliberate data quality issues.
"""

import duckdb
import os
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
random.seed(42)

# Create data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# ============================================================================
# ACT (Animal Care Tool) Database
# ============================================================================
print("Creating ACT (Animal Care Tool) database...")
act_db = duckdb.connect('data/act_system.duckdb')

# Animals table
act_db.execute("""
    CREATE TABLE animals (
        animal_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        species_id VARCHAR NOT NULL,
        enclosure_id VARCHAR NOT NULL,
        birth_date DATE,
        arrival_date DATE NOT NULL,
        health_status VARCHAR,
        keeper_id VARCHAR,
        last_vet_check DATE
    )
""")

# Species table
act_db.execute("""
    CREATE TABLE species (
        species_id VARCHAR PRIMARY KEY,
        common_name VARCHAR NOT NULL,
        scientific_name VARCHAR NOT NULL,
        habitat_type VARCHAR,
        diet_type VARCHAR,
        average_lifespan_years INTEGER
    )
""")

# Enclosures table
act_db.execute("""
    CREATE TABLE enclosures (
        enclosure_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        habitat_type VARCHAR,
        capacity INTEGER,
        temperature_celsius DECIMAL(5,2),
        humidity_percent DECIMAL(5,2)
    )
""")

# Feeding schedules table
act_db.execute("""
    CREATE TABLE feeding_schedules (
        schedule_id VARCHAR PRIMARY KEY,
        animal_id VARCHAR NOT NULL,
        feeding_time TIME NOT NULL,
        food_type VARCHAR NOT NULL,
        quantity_kg DECIMAL(5,2),
        days_of_week VARCHAR,
        last_fed DATE
    )
""")

# Keepers table
act_db.execute("""
    CREATE TABLE keepers (
        keeper_id VARCHAR PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        specialization VARCHAR,
        hire_date DATE NOT NULL
    )
""")

# Expanded species data with more variety
species_data = [
    ('SP001', 'African Lion', 'Panthera leo', 'Savanna', 'Carnivore', 15),
    ('SP002', 'Asian Elephant', 'Elephas maximus', 'Forest', 'Herbivore', 60),
    ('SP003', 'Giant Panda', 'Ailuropoda melanoleuca', 'Bamboo Forest', 'Herbivore', 20),
    ('SP004', 'Penguin', 'Spheniscus demersus', 'Aquatic', 'Carnivore', 20),
    ('SP005', 'Giraffe', 'Giraffa camelopardalis', 'Savanna', 'Herbivore', 25),
    ('SP006', 'Tiger', 'Panthera tigris', 'Forest', 'Carnivore', 15),
    ('SP007', 'Zebra', 'Equus quagga', 'Savanna', 'Herbivore', 25),
    ('SP008', 'Hippopotamus', 'Hippopotamus amphibius', 'Aquatic', 'Herbivore', 40),
    ('SP009', 'Gorilla', 'Gorilla gorilla', 'Forest', 'Herbivore', 35),
    ('SP010', 'Flamingo', 'Phoenicopterus roseus', 'Wetland', 'Omnivore', 30),
    ('SP011', 'Kangaroo', 'Macropus giganteus', 'Grassland', 'Herbivore', 20),
    ('SP012', 'Polar Bear', 'Ursus maritimus', 'Arctic', 'Carnivore', 25),
]

# Expanded enclosures
enclosures_data = [
    ('ENC001', 'Lion Pride Habitat', 'Savanna', 8, 25.5, 40.0),
    ('ENC002', 'Elephant Sanctuary', 'Forest', 6, 22.0, 60.0),
    ('ENC003', 'Panda Garden', 'Bamboo Forest', 4, 18.0, 70.0),
    ('ENC004', 'Penguin Cove', 'Aquatic', 20, 8.0, 80.0),
    ('ENC005', 'Giraffe Heights', 'Savanna', 6, 24.0, 45.0),
    ('ENC006', 'Tiger Territory', 'Forest', 4, 20.0, 55.0),
    ('ENC007', 'Zebra Plains', 'Savanna', 10, 26.0, 42.0),
    ('ENC008', 'Hippo Lagoon', 'Aquatic', 4, 24.0, 75.0),
    ('ENC009', 'Gorilla Forest', 'Forest', 8, 21.0, 65.0),
    ('ENC010', 'Flamingo Lake', 'Wetland', 30, 22.0, 70.0),
    ('ENC011', 'Kangaroo Outback', 'Grassland', 12, 20.0, 50.0),
    ('ENC012', 'Arctic Tundra', 'Arctic', 3, -5.0, 30.0),
]

# Expanded keepers
keepers_data = [
    ('KEEP001', 'Sarah', 'Johnson', 'Big Cats', '2020-01-15'),
    ('KEEP002', 'Michael', 'Chen', 'Elephants', '2019-03-20'),
    ('KEEP003', 'Emma', 'Williams', 'Pandas', '2021-06-10'),
    ('KEEP004', 'David', 'Brown', 'Aquatic', '2018-09-05'),
    ('KEEP005', 'Lisa', 'Anderson', 'Herbivores', '2020-11-12'),
    ('KEEP006', 'James', 'Wilson', 'Primates', '2019-08-22'),
    ('KEEP007', 'Maria', 'Garcia', 'Birds', '2021-02-14'),
    ('KEEP008', 'Robert', 'Taylor', 'Large Mammals', '2020-05-30'),
]

# Expanded animals data with deliberate issues:
# - Some missing birth dates (NULL)
# - Some invalid enclosure_ids (will cause referential integrity issues)
# - Some missing keeper_ids
# - Some health_status values that don't match expected values
# - Some animals with future birth dates (data quality issue)
animals_data = [
    # Normal animals
    ('AN001', 'Simba', 'SP001', 'ENC001', '2018-05-10', '2018-05-15', 'Healthy', 'KEEP001', '2024-01-15'),
    ('AN002', 'Nala', 'SP001', 'ENC001', '2019-03-22', '2019-04-01', 'Healthy', 'KEEP001', '2024-01-10'),
    ('AN003', 'Dumbo', 'SP002', 'ENC002', '2015-08-14', '2015-08-20', 'Healthy', 'KEEP002', '2024-01-20'),
    ('AN004', 'Ellie', 'SP002', 'ENC002', '2017-11-05', '2017-11-12', 'Healthy', 'KEEP002', '2024-01-18'),
    ('AN005', 'Ping', 'SP003', 'ENC003', '2020-01-20', '2020-02-01', 'Healthy', 'KEEP003', '2024-01-12'),
    ('AN006', 'Pong', 'SP003', 'ENC003', '2019-07-15', '2019-07-25', 'Healthy', 'KEEP003', '2024-01-14'),
    ('AN007', 'Flipper', 'SP004', 'ENC004', '2021-04-10', '2021-04-15', 'Healthy', 'KEEP004', '2024-01-16'),
    ('AN008', 'Splash', 'SP004', 'ENC004', '2022-06-20', '2022-06-25', 'Healthy', 'KEEP004', '2024-01-19'),
    ('AN009', 'Stretch', 'SP005', 'ENC005', '2019-09-12', '2019-09-18', 'Healthy', 'KEEP005', '2024-01-17'),
    ('AN010', 'Spot', 'SP006', 'ENC006', '2020-12-05', '2020-12-10', 'Healthy', 'KEEP001', '2024-01-13'),
    # More animals
    ('AN011', 'Stripes', 'SP007', 'ENC007', '2021-03-15', '2021-03-20', 'Healthy', 'KEEP005', '2024-01-11'),
    ('AN012', 'Ziggy', 'SP007', 'ENC007', '2020-08-22', '2020-08-28', 'Healthy', 'KEEP005', '2024-01-09'),
    ('AN013', 'Hippo', 'SP008', 'ENC008', '2016-11-10', '2016-11-15', 'Healthy', 'KEEP004', '2024-01-21'),
    ('AN014', 'Koko', 'SP009', 'ENC009', '2018-02-14', '2018-02-20', 'Healthy', 'KEEP006', '2024-01-08'),
    ('AN015', 'Pinkie', 'SP010', 'ENC010', '2022-05-05', '2022-05-10', 'Healthy', 'KEEP007', '2024-01-15'),
    ('AN016', 'Flamingo2', 'SP010', 'ENC010', '2021-09-12', '2021-09-18', 'Healthy', 'KEEP007', '2024-01-12'),
    ('AN017', 'Joey', 'SP011', 'ENC011', '2020-04-20', '2020-04-25', 'Healthy', 'KEEP005', '2024-01-14'),
    ('AN018', 'Snowball', 'SP012', 'ENC012', '2019-12-01', '2019-12-05', 'Healthy', 'KEEP008', '2024-01-16'),
    # Data quality issues - missing birth dates
    ('AN019', 'Mystery', 'SP001', 'ENC001', None, '2023-01-10', 'Healthy', 'KEEP001', '2024-01-10'),
    ('AN020', 'Unknown', 'SP002', 'ENC002', None, '2022-06-15', 'Healthy', 'KEEP002', '2024-01-18'),
    # Data quality issues - missing keeper
    ('AN021', 'Orphan', 'SP005', 'ENC005', '2021-07-20', '2021-07-25', 'Healthy', None, '2024-01-12'),
    # Data quality issues - invalid health status
    ('AN022', 'Sickly', 'SP006', 'ENC006', '2020-05-10', '2020-05-15', 'Sick', 'KEEP001', '2024-01-10'),
    ('AN023', 'Recovering', 'SP003', 'ENC003', '2019-10-05', '2019-10-10', 'Recovering', 'KEEP003', '2024-01-11'),
    # Data quality issues - invalid enclosure (doesn't exist)
    ('AN024', 'Lost', 'SP007', 'ENC999', '2021-01-15', '2021-01-20', 'Healthy', 'KEEP005', '2024-01-13'),
    # Data quality issues - future birth date (impossible)
    ('AN025', 'TimeTraveler', 'SP004', 'ENC004', '2025-12-25', '2023-01-01', 'Healthy', 'KEEP004', '2024-01-15'),
    # More normal animals to have enough data
    ('AN026', 'Leo', 'SP001', 'ENC001', '2017-08-10', '2017-08-15', 'Healthy', 'KEEP001', '2024-01-14'),
    ('AN027', 'Tusker', 'SP002', 'ENC002', '2014-03-20', '2014-03-25', 'Healthy', 'KEEP002', '2024-01-19'),
    ('AN028', 'Bamboo', 'SP003', 'ENC003', '2021-11-10', '2021-11-15', 'Healthy', 'KEEP003', '2024-01-17'),
    ('AN029', 'Waddle', 'SP004', 'ENC004', '2023-02-14', '2023-02-18', 'Healthy', 'KEEP004', '2024-01-20'),
    ('AN030', 'Necky', 'SP005', 'ENC005', '2020-09-05', '2020-09-10', 'Healthy', 'KEEP005', '2024-01-16'),
]

# Expanded feeding schedules with some issues:
# - Some with invalid feeding times (outside 6-20 range)
# - Some with negative quantities
# - Some with NULL food_type
feeding_schedules_data = [
    # Normal schedules
    ('FS001', 'AN001', '08:00:00', 'Raw Meat', 5.5, 'Mon,Wed,Fri,Sun', '2024-01-21'),
    ('FS002', 'AN001', '17:00:00', 'Raw Meat', 5.5, 'Mon,Wed,Fri,Sun', '2024-01-21'),
    ('FS003', 'AN002', '08:30:00', 'Raw Meat', 4.8, 'Tue,Thu,Sat', '2024-01-20'),
    ('FS004', 'AN003', '09:00:00', 'Hay & Vegetables', 50.0, 'Daily', '2024-01-21'),
    ('FS005', 'AN004', '09:15:00', 'Hay & Vegetables', 45.0, 'Daily', '2024-01-21'),
    ('FS006', 'AN005', '10:00:00', 'Bamboo', 15.0, 'Daily', '2024-01-21'),
    ('FS007', 'AN006', '10:00:00', 'Bamboo', 15.0, 'Daily', '2024-01-21'),
    ('FS008', 'AN007', '11:00:00', 'Fish', 2.0, 'Daily', '2024-01-21'),
    ('FS009', 'AN008', '11:30:00', 'Fish', 1.8, 'Daily', '2024-01-21'),
    ('FS010', 'AN009', '08:00:00', 'Leaves & Fruits', 12.0, 'Daily', '2024-01-21'),
    ('FS011', 'AN010', '09:00:00', 'Raw Meat', 6.0, 'Mon,Wed,Fri,Sun', '2024-01-20'),
    ('FS012', 'AN011', '07:30:00', 'Grass', 8.0, 'Daily', '2024-01-21'),
    ('FS013', 'AN012', '08:00:00', 'Grass', 7.5, 'Daily', '2024-01-21'),
    ('FS014', 'AN013', '10:00:00', 'Vegetables', 20.0, 'Daily', '2024-01-21'),
    ('FS015', 'AN014', '09:00:00', 'Fruits & Vegetables', 10.0, 'Daily', '2024-01-21'),
    ('FS016', 'AN015', '08:30:00', 'Shrimp', 0.5, 'Daily', '2024-01-21'),
    ('FS017', 'AN016', '09:00:00', 'Shrimp', 0.5, 'Daily', '2024-01-21'),
    ('FS018', 'AN017', '07:00:00', 'Grass & Leaves', 5.0, 'Daily', '2024-01-21'),
    ('FS019', 'AN018', '10:00:00', 'Fish & Seal', 8.0, 'Daily', '2024-01-21'),
    ('FS020', 'AN019', '05:00:00', 'Raw Meat', 5.0, 'Daily', '2024-01-21'),
    ('FS021', 'AN020', '21:00:00', 'Hay', 40.0, 'Daily', '2024-01-21'),
    ('FS022', 'AN021', '09:00:00', 'Leaves', -5.0, 'Daily', '2024-01-21'),
    ('FS024', 'AN023', '10:00:00', 'Bamboo', 12.0, 'Daily', '2024-01-21'),
    ('FS025', 'AN026', '08:00:00', 'Raw Meat', 5.8, 'Mon,Wed,Fri,Sun', '2024-01-21'),
    ('FS026', 'AN027', '09:00:00', 'Hay & Vegetables', 55.0, 'Daily', '2024-01-21'),
    ('FS027', 'AN028', '10:00:00', 'Bamboo', 16.0, 'Daily', '2024-01-21'),
    ('FS028', 'AN029', '11:00:00', 'Fish', 2.2, 'Daily', '2024-01-21'),
    ('FS029', 'AN030', '08:00:00', 'Leaves & Fruits', 13.0, 'Daily', '2024-01-21'),
]

act_db.executemany("INSERT INTO species VALUES (?, ?, ?, ?, ?, ?)", species_data)
act_db.executemany("INSERT INTO enclosures VALUES (?, ?, ?, ?, ?, ?)", enclosures_data)
act_db.executemany("INSERT INTO keepers VALUES (?, ?, ?, ?, ?)", keepers_data)
act_db.executemany("INSERT INTO animals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", animals_data)
act_db.executemany("INSERT INTO feeding_schedules VALUES (?, ?, ?, ?, ?, ?, ?)", feeding_schedules_data)

act_db.close()
print("ACT database created successfully!")

# ============================================================================
# VRM (Visitor Relationship Management) Database
# ============================================================================
print("\nCreating VRM (Visitor Relationship Management) database...")
vrm_db = duckdb.connect('data/vrm_system.duckdb')

# Visitors table
vrm_db.execute("""
    CREATE TABLE visitors (
        visitor_id VARCHAR PRIMARY KEY,
        ticket_id VARCHAR NOT NULL,
        visit_date DATE NOT NULL,
        entry_time TIME,
        exit_time TIME,
        visitor_type VARCHAR,
        group_size INTEGER,
        total_spent_usd DECIMAL(10,2)
    )
""")

# Events table
vrm_db.execute("""
    CREATE TABLE events (
        event_id VARCHAR PRIMARY KEY,
        event_name VARCHAR NOT NULL,
        event_date DATE NOT NULL,
        start_time TIME,
        end_time TIME,
        location VARCHAR,
        capacity INTEGER,
        tickets_sold INTEGER,
        revenue_usd DECIMAL(10,2)
    )
""")

# Tickets table
vrm_db.execute("""
    CREATE TABLE tickets (
        ticket_id VARCHAR PRIMARY KEY,
        purchase_date DATE NOT NULL,
        ticket_type VARCHAR NOT NULL,
        price_usd DECIMAL(10,2) NOT NULL,
        visitor_id VARCHAR,
        event_id VARCHAR
    )
""")

# Visitor feedback table
vrm_db.execute("""
    CREATE TABLE visitor_feedback (
        feedback_id VARCHAR PRIMARY KEY,
        visitor_id VARCHAR,
        visit_date DATE,
        rating INTEGER,
        feedback_text VARCHAR,
        submitted_date DATE
    )
""")

# Generate expanded data for 30 days (1 month) with more variety and issues
base_date = datetime(2024, 1, 1)
visitors_data = []
tickets_data = []
events_data = []
feedback_data = []

# Generate data for 30 days
for day in range(30):
    visit_date = base_date + timedelta(days=day)
    date_str = visit_date.strftime('%Y-%m-%d')
    
    # Create 2-4 events per day
    num_events = random.randint(2, 4)
    for e in range(num_events):
        event_id = f'EVT{day:03d}{e:01d}'
        event_name = random.choice([
            'Lion Feeding Show', 'Elephant Encounter', 'Penguin Parade',
            'Giraffe Feeding', 'Tiger Talk', 'Panda Story Time',
            'Zebra Safari', 'Hippo Splash', 'Gorilla Watch', 'Flamingo Dance'
        ])
        tickets_sold = random.randint(30, 120)
        revenue = tickets_sold * random.uniform(15, 30)
        events_data.append((
            event_id,
            event_name,
            date_str,
            f"{random.randint(10, 14):02d}:00:00",
            f"{random.randint(15, 17):02d}:00:00",
            random.choice(['Main Stage', 'Lion Arena', 'Penguin Cove', 'Elephant Plaza', 'Savanna View']),
            random.randint(50, 150),
            tickets_sold,
            round(revenue, 2)
        ))
    
    # Create 20 - 40 visitors per day (more on weekends)
    is_weekend = visit_date.weekday() >= 5
    base_visitors = 20 if is_weekend else 40
    num_visitors = random.randint(base_visitors, base_visitors + 20)
    
    for v in range(num_visitors):
        visitor_id = f'VIS{day:03d}{v:05d}'
        ticket_id = f'TKT{day:03d}{v:05d}'
        visitor_type = random.choice(['Adult', 'Child', 'Senior', 'Student'])
        group_size = random.randint(1, 6)
        total_spent = round(random.uniform(25, 150), 2)
        
        visitors_data.append((
            visitor_id,
            ticket_id,
            date_str,
            f"{random.randint(9, 11):02d}:{random.randint(0, 59):02d}:00",
            f"{random.randint(15, 18):02d}:{random.randint(0, 59):02d}:00",
            visitor_type,
            group_size,
            total_spent
        ))
        
        ticket_price = round(total_spent / group_size, 2)
        tickets_data.append((
            ticket_id,
            (visit_date - timedelta(days=random.randint(0, 14))).strftime('%Y-%m-%d'),
            visitor_type,
            ticket_price,
            visitor_id,
            events_data[-1][0] if events_data and random.random() > 0.4 else None
        ))
        
        # Some visitors leave feedback (20% chance)
        if random.random() > 0.8:
            feedback_data.append((
                f'FB{day:03d}{v:05d}',
                visitor_id,
                date_str,
                random.randint(1, 5),
                random.choice([
                    'Great experience!', 'Loved the animals', 'Will come again',
                    'Amazing show', 'Kids had fun', 'Educational and fun',
                    'Too crowded', 'Expensive', 'Not enough animals', 'Poor service'
                ]),
                (visit_date + timedelta(days=random.randint(0, 3))).strftime('%Y-%m-%d')
            ))
    
    if day % 10 == 0:
        visitor_id = f'VIS{day:03d}BAD01'
        ticket_id = f'TKT{day:03d}BAD01'
        visitors_data.append((
            visitor_id,
            ticket_id,
            date_str,
            '10:00:00',
            '16:00:00',
            'Adult',
            1,
            -50.00
        ))
        tickets_data.append((
            ticket_id,
            date_str,
            'Adult',
            -50.00,
            visitor_id,
            None
        ))
        
        visitor_id2 = f'VIS{day:03d}BAD02'
        ticket_id2 = f'TKT{day:03d}BAD02'
        visitors_data.append((
            visitor_id2,
            ticket_id2,
            date_str,
            '11:00:00',
            '17:00:00',
            'Adult',
            2,
            60.00
        ))
        tickets_data.append((
            ticket_id2,
            date_str,
            'Child',
            30.00,
            visitor_id2,
            None
        ))
        
        event_id = f'EVT{day:03d}BAD'
        events_data.append((
            event_id,
            'Overbooked Event',
            date_str,
            '12:00:00',
            '13:00:00',
            'Main Stage',
            50,
            75,
            round(75 * 20, 2)
        ))

vrm_db.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", events_data)
vrm_db.executemany("INSERT INTO visitors VALUES (?, ?, ?, ?, ?, ?, ?, ?)", visitors_data)
vrm_db.executemany("INSERT INTO tickets VALUES (?, ?, ?, ?, ?, ?)", tickets_data)
vrm_db.executemany("INSERT INTO visitor_feedback VALUES (?, ?, ?, ?, ?, ?)", feedback_data)

vrm_db.close()
print("VRM database created successfully!")

print("\n" + "="*60)
print("Database setup complete!")
print("="*60)
print("\nCreated databases:")
print("  - data/act_system.duckdb (Animal Care Tool)")
print("  - data/vrm_system.duckdb (Visitor Relationship Management)")
print("\nNote: The databases contain deliberate data quality issues")
print("that students will encounter and need to handle in their models.")

