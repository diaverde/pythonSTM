import csv, pyodbc

SERVER = '127.0.01'
DATABASE = 'STMInfo'
USERNAME = 'sa'
PASSWORD = 'R1thm?24601'

connectionString = f'Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{SERVER},1433;Database={DATABASE};Uid={USERNAME};Pwd={PASSWORD};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;'

conn = pyodbc.connect(connectionString)
cur = conn.cursor()

# Routes
sqlQuery = """
    "CREATE TABLE routes (
    route_id INT PRIMARY KEY,
    agency_id VARCHAR(255),
    route_short_name VARCHAR(255),
    route_long_name VARCHAR(255),
    route_type VARCHAR(255),
    route_url VARCHAR(255),
    route_color VARCHAR(255),
    route_text_color VARCHAR(255)
    );
    """
cur.execute(sqlQuery)
with open('gtfs_stm/routes.txt','r') as fin: # `with` statement available in 2.5+
    # csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin) # comma is default delimiter    
    """
    for i in dr:
        print(i)
        print(i['route_id'])
        print(int(i['route_id']))
    """
    routes_db = [(int(i['route_id']),i['agency_id'],i['route_short_name'],i['route_long_name'],i['route_type'],i['route_url'],i['route_color'],i['route_text_color']) for i in dr]

sqlQuery = """
    INSERT INTO routes (route_id,agency_id,route_short_name,route_long_name,route_type,route_url,route_color,route_text_color)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
cur.executemany(sqlQuery, routes_db)

# Stops
sqlQuery = """
    CREATE TABLE stops (
    stop_id VARCHAR(255) PRIMARY KEY,
    stop_code VARCHAR(255),
    stop_name VARCHAR(255),
    stop_lat VARCHAR(255),
    stop_lon VARCHAR(255),
    stop_url VARCHAR(255),
    location_type VARCHAR(255),
    parent_station VARCHAR(255),
    wheelchair_boarding VARCHAR(10)
    );
    """
cur.execute(sqlQuery)
with open('gtfs_stm/stops.txt','r') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter    
    #for i in dr:
        #print(i)
    stops_db = [(i['stop_id'],i['stop_code'],i['stop_name'],i['stop_lat'],i['stop_lon'],i['stop_url'],i['location_type'],i['parent_station'],i['wheelchair_boarding']) for i in dr]

sqlQuery = """
    INSERT INTO stops (stop_id,stop_code,stop_name,stop_lat,stop_lon,stop_url,location_type,parent_station,wheelchair_boarding)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
cur.executemany(sqlQuery, stops_db)

# Shapes
sqlQuery = """
    CREATE TABLE shapes (
    shape_id INT,
    shape_pt_lat VARCHAR(255),
    shape_pt_lon VARCHAR(255),
    shape_pt_sequence INT);
    """
cur.execute(sqlQuery)
with open('gtfs_stm/shapes.txt','r') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter    
    #for i in dr:
        #print(i)
    shapes_db = [(int(i['shape_id']),i['shape_pt_lat'],i['shape_pt_lon'],int(i['shape_pt_sequence'])) for i in dr]

sqlQuery = """
    INSERT INTO shapes (shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence)
    VALUES (?, ?, ?, ?);
    """
cur.executemany(sqlQuery, shapes_db)

# Services
sqlQuery = """
    CREATE TABLE services (
    service_id VARCHAR(255) PRIMARY KEY,
    monday VARCHAR(7),
    tuesday VARCHAR(7),
    wednesday VARCHAR(7),
    thursday VARCHAR(7),
    friday VARCHAR(7),
    saturday VARCHAR(7),
    sunday VARCHAR(7),
    start_date VARCHAR(255),
    end_date VARCHAR(255)
    );
    """
cur.execute(sqlQuery)
with open('gtfs_stm/calendar.txt','r') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter    
    #for i in dr:
        #print(i)
    services_db = [(i['service_id'],i['monday'],i['tuesday'],i['wednesday'],i['thursday'],i['friday'],i['saturday'],i['sunday'],i['start_date'],i['end_date']) for i in dr]
    
sqlQuery = """
    INSERT INTO services (service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
cur.executemany(sqlQuery, services_db)

# Trips
sqlQuery = """
    CREATE TABLE trips (
    route_id INT,
    service_id VARCHAR(255),
    trip_id INT PRIMARY KEY,
    trip_headsign VARCHAR(255),
    direction_id VARCHAR(255),
    wheelchair_accessible VARCHAR(7),
    note_fr VARCHAR(255),
    note_en VARCHAR(255),
    FOREIGN KEY (route_id) REFERENCES routes(route_id),
    FOREIGN KEY (service_id) REFERENCES services(service_id)
    );
    """
cur.execute(sqlQuery)
with open('gtfs_stm/trips.txt','r') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter    
    #for i in dr:
        #print(i)
    trips_db = [(int(i['route_id']),i['service_id'],int(i['trip_id']),i['trip_headsign'],i['direction_id'],i['wheelchair_accessible'],i['note_fr'],i['note_en']) for i in dr]

sqlQuery = """
    INSERT INTO trips (route_id,service_id,trip_id,trip_headsign,direction_id,wheelchair_accessible,note_fr,note_en)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
cur.executemany(sqlQuery, trips_db)

# Stop times
sqlQuery = """
    CREATE TABLE stop_times (
    trip_id INT,
    arrival_time VARCHAR(255),
    departure_time VARCHAR(255),
    stop_id VARCHAR(255),
    stop_sequence INT
    FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
    FOREIGN KEY (stop_id) REFERENCES stops(stop_id));
    """
cur.execute(sqlQuery)
with open('gtfs_stm/stop_times.txt','r') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter    
    #for i in dr:
        #print(i)
    stop_times_db = [(int(i['trip_id']),i['arrival_time'],i['departure_time'],i['stop_id'],int(i['stop_sequence'])) for i in dr]

sqlQuery = """
    INSERT INTO stop_times (trip_id,arrival_time,departure_time,stop_id,stop_sequence)
    VALUES (?, ?, ?, ?, ?);
    """    
cur.executemany(sqlQuery, stop_times_db)

conn.commit()
conn.close()