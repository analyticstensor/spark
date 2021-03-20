# Read config file

import configparser
import os

class ReadProperties:
    """ Class to read database properties. 
    paramater:
    conf_file: configuration file
    section: section on configuration file.
    """
    CONF_DIR = '/Users/kcmahesh/company/training/Spark/Chapter_5_Join/script/conf/'

    def __init__(self, conf_file, section):
        self.conf_file = os.path.join(ReadProperties.CONF_DIR, conf_file)
        self.section = section

    def get_properties(self):
        """ Return dictionary of from config file. """
        db_properties = dict()
        config = configparser.ConfigParser()
        config.read(self.conf_file)
        sections = config.sections()
        if self.section in sections:
            db_prop = config[self.section]
            db_properties['url'] = db_prop['url']
            db_properties['database'] = db_prop['database']
            db_properties['schema'] = db_prop['schema']
            db_properties['user'] = db_prop['user']
            db_properties['password'] = db_prop['password']
            db_properties['serverTimezone'] = db_prop['serverTimezone']
        else:
            print("Section {} doesn't exists in {} file.".format(self.section, self.conf_file))
        return db_properties
