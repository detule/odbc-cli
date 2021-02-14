from threading import Lock, Event, Thread

class DbMetadata():
    def __init__(self) -> None:
        self._lock = Lock()
        self._dbmetadata = {'table': {}, 'view': {}, 'function': {},
                'datatype': {}}

    def extend_catalogs(self, names: list) -> None:
        with self._lock:
            for metadata in self._dbmetadata.values():
                for catalog in names:
                    metadata[catalog.lower()] = (catalog, {})
        return

    def get_catalogs(self, obj_type: str = "table", cased: bool = True) -> list:
        """ Retrieve catalogs as the keys for _dbmetadata[obj_type]
            If no keys are found it returns None.
        """
        with self._lock:
            if cased:
                res = [casedkey for casedkey, mappedvalue in self._dbmetadata[obj_type].values()]
            else:
                res = list(self._dbmetadata[obj_type].keys())


        if len(res) == 0:
            return None

        return res

    def extend_schemas(self, catalog, names: list) -> None:
        """ This method will force/create [catalog] dictionary
            in the event that len(names) > 0, and overwrite if
            anything was there to begin with.
        """
        catlower = catalog.lower()
        cat_cased = catalog
        if len(names):
            with self._lock:
                for metadata in self._dbmetadata.values():
                    # Preserve casing if an entry already there
                    if catlower in metadata.keys() and len(metadata[catlower]):
                        cat_cased = metadata[catlower][0]
                    metadata[catlower] = (cat_cased, {})
                    for schema in names:
                        metadata[catlower][1][schema.lower()] = (schema, {})
        return

    def get_schemas(self, catalog: str, obj_type: str = "table", cased: bool = True) -> list:
        """ Retrieve schemas as the keys for _dbmetadata[obj_type][catalog]
            If catalog is not part of the _dbmetadata[obj_type] keys will return
            None.
        """

        catlower = catalog.lower()
        cats = self.get_catalogs(obj_type = obj_type, cased = False)
        if cats is None or catlower not in cats:
            return None

        with self._lock:
            if cased:
                res = [casedkey for casedkey, mappedvalue in self._dbmetadata[obj_type][catlower][1].values()]
            else:
                res = list(self._dbmetadata[obj_type][catlower][1].keys())


        return res

    def extend_objects(self, catalog, schema, names: list, obj_type: str) -> None:
        catlower = catalog.lower()
        schlower = schema.lower()
        if len(names):
            with self._lock:
                for otype in self._dbmetadata.keys():
                    # Loop over tables, views, functions
                    if catlower not in self._dbmetadata[otype].keys():
                        self._dbmetadata[otype][catlower] = (catalog, {})
                    if schlower not in self._dbmetadata[otype][catlower][1].keys():
                        self._dbmetadata[otype][catlower][1][schlower] = (schema, {})
                for obj in names:
                    self._dbmetadata[obj_type][catlower][1][schlower][1][obj.lower()] = (obj, {})
        # If we passed nothing then take out that element entirely out
        # of the dict
        else:
            with self._lock:
                del self._dbmetadata[obj_type][catlower][1][schlower]

        return

    def get_objects(self, catalog: str, schema: str, obj_type: str = "table") -> list:
        """ Retrieve objects as the keys for _dbmetadata[obj_type][catalog][schema]
            If catalog is not part of the _dbmetadata[obj_type] keys, or schema
            not one of the keys in _dbmetadata[obj_type][catalog] will return None
        """

        catlower = catalog.lower()
        schlower = schema.lower()
        schemas = self.get_schemas(catalog = catalog, obj_type = obj_type, cased = False)
        if schemas is None or schlower not in schemas:
            return None

        with self._lock:
            res = [casedkey for casedkey, mappedvalue in self._dbmetadata[obj_type][catlower][1][schlower][1].values()]

        return list(res)

    def reset_metadata(self) -> None:
        with self._lock:
            self._dbmetadata = {'table': {}, 'view': {}, 'function': {},
                               'datatype': {}}

    @property
    def data(self) -> dict:
        return self._dbmetadata
