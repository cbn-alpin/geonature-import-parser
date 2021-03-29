import os
import re
import ast
from configparser import ConfigParser, ExtendedInterpolation, NoOptionError


class Config:
    """Handle configuration parameters."""

    configParser = ConfigParser(interpolation=None)

    section_pattern = re.compile(r"(?:^[#;][^$]+$)*^\[[^]]+\]$", re.MULTILINE)
    initialized = False
    default_section_name = 'DEFAULT'

    local_config_path = (os.path.join(os.environ['IMPORT_PARSER.PATHES.APP.CONFIG'], 'settings.ini'))
    default_config_file_path = (os.path.join(os.environ['IMPORT_PARSER.PATHES.APP.CONFIG'], 'settings.default.ini'))
    local_shared_config_file_path = (os.path.join(os.environ['IMPORT_PARSER.PATHES.SHARED.CONFIG'], 'settings.ini'))
    default_shared_config_file_path = (os.path.join(os.environ['IMPORT_PARSER.PATHES.SHARED.CONFIG'], 'settings.default.ini'))

    nomenclatures_config_file_path = (os.path.join(os.environ['IMPORT_PARSER.PATHES.APP.CONFIG'], 'nomenclatures.ini'))

    @classmethod
    def _initialize(cls):
        """ Start config by reading all settings.ini files. """
        cls.initialized = True # Must be in first place
        config_files = [
            cls.default_shared_config_file_path,
            cls.local_shared_config_file_path,
            cls.default_config_file_path,
            cls.local_config_path
        ]
        for cfg_file in config_files :
            cls.load(cfg_file)

        return cls

    @classmethod
    def _splitKey(cls, key):
        splited_key = key.split('.')
        if len(splited_key) == 1 :
            section = cls.default_section_name
            option = key
        elif len(splited_key) == 2 :
            section, option = map(str, splited_key)
        elif len(splited_key) > 2 :
            section = splited_key[0]
            option = '.'.join(splited_key[1:])
        return [section, option]

    @classmethod
    def _hasNeedToEval(cls, value):
        need_eval = False
        if value.startswith('[') and value.endswith(']'):
            need_eval = True
        elif value.startswith('{') and value.endswith('}'):
            need_eval = True
        return need_eval

    @classmethod
    def load(cls, path):
        if not cls.initialized:
            cls._initialize()

        try:
            with open(path, 'r') as f:
                text = f.read()
        except IOError:
            pass
        else:
            if text != '' and cls.section_pattern.search(text) == None:
                text = "["+ cls.default_section_name + "]\n" + text
            cls.configParser.read_string(text)

    @classmethod
    def has(cls, key):
        """ Return True if key exists, else False. """
        if not cls.initialized:
            cls._initialize()

        section, option = map(str, cls._splitKey(key))
        return cls.configParser.has_option(section, option)


    @classmethod
    def get(cls, key):
        """
        Get a value of an option from all settings.ini files loaded.

        In key, you can use dot to separate section and parameter name like this : section.parameter
        Double quoted characters will be stripped.
        If value is surrounded by [] then a list will be return.
        If value is surrounded by {} then a dict will be return.
        If value is 0, no, false or off then False (boolean) will be return.
        If value is 1, yes, true or on then True (boolean) will be return.
        If option was not found, a value None is returned.
        """
        if not cls.initialized:
            cls._initialize()

        section, option = map(str, cls._splitKey(key))
        try:
            value = cls.configParser.get(section, option)
        except NoOptionError:
            return None

        # Strip double quotes
        value = value.strip('"')
        # Make an eval of the value if value is surrounded by [] or {}
        if cls._hasNeedToEval(value):
            value = ast.literal_eval(value)
        elif value.lower() in ['1', 'yes', 'true', 'on',]:
            value = True
        elif value.lower() in ['0', 'no', 'false', 'off',]:
            value = False

        return value

    @classmethod
    def setParameter(cls, key, value):
        """
        Set a value.

        In key, you can use dot to separate section and parameter name like this : section.parameter
        """
        if not cls.initialized:
            cls._initialize()

        section, option = map(str, cls._splitKey(key))

        if not cls.configParser.has_section(section):
            cls.configParser.add_section(section)

        cls.configParser.set(section, option, value)

    @classmethod
    def getSection(cls, section):
        """
        Get alls value-key pairs for the given section.

        All default section items will be removed.
        """
        if not cls.initialized:
            cls._initialize()

        # Extract only section asked options without default section options
        filtered_items = {}
        for item in cls.configParser.items(section):
            if item[0] not in cls.configParser.defaults():
                filtered_items[item[0]] = item[1]
        return filtered_items

    @classmethod
    def getConfigParser(cls):
        """Get ConfigParser class itself."""
        if not cls.initialized:
            cls._initialize()

        return cls.configParser
