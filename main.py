import pandas as pd
from sqlalchemy import create_engine
from config import config_db


def get_sql_engine():
    """
    creates the DB engine for pandas to use.
    :return:
    """
    params = config_db()
    user = params['user']
    password = params['password']
    host = params['host']
    port = params.get('port', 5432)
    db = params['dbname']
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')
    return engine


def write_to_postgres(df, table_name):
    """
    This function writes the dataframe to a postgres table. It uses the to_sql method from pandas,
    which is a wrapper around psycopg2.

    @param df: The dataframe to write.
    @param table_name: The name of the table to write to.
    @param conn: The connection to the postgres database.
    """
    engine = get_sql_engine()
    try:
        df.index.name = "id"
        df.to_sql(table_name, engine, if_exists='replace', index=True)
        print(f"Data written to {table_name} successfully.")
    except Exception as e:
        print(f"Error writing data to {table_name}: {e}")


def parse_alarm_file(file_path):
    """
    This function parses the alarm file. Because the format isn't using headers and is left
    to right, parsing had to be done manually, then loaded into a pandas dataframe.

    @param file_path: The path to the alarm file.
    @return: A pandas dataframe containing the parsed alarm data.
    """
    with open(file_path, "r", encoding="utf-8-sig") as file:
        lines = file.read().strip().split("\n")

    records = []
    record = {}
    last_key = None

    for line in lines:
        stripped = line.strip()
        if stripped == "":
            if record:
                records.append(record)
                record = {}
                last_key = None
        elif ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            record[key] = value
            last_key = key
        elif last_key:
            record[last_key] += "\n" + line.strip()

    return pd.DataFrame(records)


def get_site(node_name):
    """
    This function extracts the site code from the node name. it uses isdigit and isalpha to check
    for the first instance of a digit, and the last a letter.

    @param node_name: The node name.
    @return: The site code - starting with a number and ending with a letter.
    """
    start = None
    end = None

    for i, char in enumerate(node_name):
        if start is None and char.isdigit():
            start = i
        if char.isalpha():
            end = i

    # If both start and end are found, return the stuff in the middle
    if start is not None and end is not None and end >= start:
        return node_name[start:end + 1]
    return None


def get_location_second_subnetwork():
    pass

def main(filename="alarms.txt"):
    required_columns = ["NodeName",
                        "specificProblem",
                        "eventTime",
                        "problemText",
                        "alarmState",
                        "alarmId",
                        "probableCause",
                        "eventType"]

    df = parse_alarm_file(filename)

    # Finding the location from the second subnetwork would have to happen here
    # because it is under the object reference header and that isn't in the final df
    # df = get_location_second_subnetwork(df)

    df = df[required_columns]

    # Convert the times to date time objects, so I can convert them to EST
    df["eventTime"] = pd.to_datetime(df["eventTime"], errors="coerce")

    # Convert to EST in the new loading time column
    df["loading_time"] = df["eventTime"].dt.tz_localize("UTC").dt.tz_convert("America/New_York")

    # Create the site column
    df["site"] = df["NodeName"].apply(get_site)

    # Track duplicates for fun
    dupes = df[df.duplicated(keep=False)]

    # removes any duplicates - Thanks pandas
    df = df.drop_duplicates()

    # write it to postgres
    write_to_postgres(df, "alarms")

    # this tracks the duplicates - a 4fun thing to visually see them
    write_to_postgres(dupes, "duplicates")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
