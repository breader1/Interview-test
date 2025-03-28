from configparser import ConfigParser

def config_db(filename ="db.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(filename, encoding="utf-8-sig")
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in the {filename} file")
    return db
